"""IDE Session Service — On-demand code-server sessions from S3 snapshots.

Manages the lifecycle of restored IDE sessions:
  1. Check availability (snapshot or Gitea repo exists)
  2. Start session (provision VM, extract snapshot, boot code-server)
  3. Track session state (restoring → active → idle → expired)
  4. TTL enforcement (idle timeout + max lifetime)
  5. Manual teardown

Session state is stored in ``jobs.context.ide_session`` via atomic JSONB
merges. The cockpit polls ``GET /api/jobs/{job_id}/ide`` to drive the
IDE button UI.
"""

import asyncio
import json
import logging
import os
import re
import shlex
from datetime import datetime, timezone, timedelta
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID, uuid4

from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    authorize_job_repository_transport,
)

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.blocking_effect import joined_async_call
from orchestrator.services.container_provisioner import (
    WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY,
    WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY,
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceRuntimeAttestation,
    WorkspaceRuntimeAuthorityError,
)
from orchestrator.services.ide_proxy import contain_ide_status_for, contained_ide_status
from orchestrator.services.ssh_helpers import (
    EXTRACT_HOME_REMOTE_CMD,
    SSHHostKeyVerificationError,
    orchestrator_can_reach,
    pinned_agent_ssh_command,
    stream_extract_snapshot,
)
from orchestrator.services.subprocess_effect import (
    communicate_bounded,
    create_owned_subprocess_exec,
    stop_and_reap,
    wait_bounded,
)
from orchestrator.services.restore_work_lease import (
    RestoreWorkLeaseHeartbeat,
    RestoreWorkLeaseLost,
)
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from shared.runtime.core.managed_repository import (
    managed_repository_agent_launch_command,
    managed_repository_agent_retirement_command,
    managed_repository_agent_zero_command,
)

logger = logging.getLogger(__name__)

_VM_READY_TIMEOUT_SECONDS = 420


def _canonical_runtime(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return value if str(UUID(value)) == value else None
    except (TypeError, ValueError):
        return None


def _settled_ide_restore_runtime(
    session: Any,
    creation: Any,
    *,
    retired_runtime: str | None = None,
) -> str | None:
    """Return exact IDE runtime B only when its durable receipt matches."""

    if not isinstance(session, dict) or not isinstance(creation, dict):
        return None
    if (
        str(creation.get("operation_kind") or "") != "restore"
        or str(creation.get("result_kind") or "") != "settled"
        or creation.get("settled_at") is None
        or session.get(WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY)
        != str(creation.get("id") or "")
        or session.get(WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY)
        != str(creation.get("claim_token") or "")
    ):
        return None
    runtime = _canonical_runtime(session.get(WORKSPACE_RUNTIME_INCARNATION_KEY))
    receipt_runtime = _canonical_runtime(creation.get("runtime_incarnation"))
    if runtime is None or runtime != receipt_runtime or runtime == retired_runtime:
        return None
    return runtime


def _claimed_ide_restore_work(
    restore_work: Any,
    *,
    runtime_incarnation: str,
    claimant: str,
) -> bool:
    """Validate one exact-B IDE post-create work lease."""

    if not isinstance(restore_work, dict):
        return False
    try:
        str(UUID(str(restore_work.get("id"))))
        work_runtime = str(UUID(str(restore_work.get("runtime_incarnation"))))
        work_token = int(restore_work.get("restore_work_claim_token"))
    except (TypeError, ValueError):
        return False
    return bool(
        work_runtime == runtime_incarnation
        and work_token > 0
        and restore_work.get("restore_work_claimed_by") == claimant
        and restore_work.get("restore_work_completed_at") is None
    )


def _build_code_server_url(
    job_id: str, folder: str = "/home/agent-host/workspace"
) -> str:
    """Build the proxy-routed code-server URL for a job."""
    proxy_base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
    return f"{proxy_base}/api/ide/{job_id}/proxy/?folder={folder}"


class IdeSessionService:
    """Manages on-demand IDE sessions backed by S3 environment snapshots.

    Coordinates between SnapshotService (S3 download), VMProvisioner
    (VM creation), and the management daemon (code-server health).
    """

    def __init__(self) -> None:
        self._db: Any = None
        self._snapshot_service: Any = None
        self._vm_provisioner: Any = None
        self._workspace_recovery_store: Any = None
        self._container_provisioner: Any = None
        self._gitea_client: Any = None
        # Background restores are process-local, but their runtime identity is
        # durable.  The map prevents duplicate work from concurrent requests
        # handled by this process; after a process restart, start_session()
        # validates the durable B receipt and deliberately replays it.
        self._restore_tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def idle_timeout_minutes(self) -> int:
        return int(os.environ.get("IDE_SESSION_IDLE_TIMEOUT", "30"))

    @property
    def max_lifetime_minutes(self) -> int:
        return int(os.environ.get("IDE_SESSION_MAX_LIFETIME", "240"))

    @property
    def max_concurrent_per_user(self) -> int:
        return int(os.environ.get("IDE_MAX_CONCURRENT_PER_USER", "2"))

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def connect(
        self,
        db: Any,
        snapshot_service: Any,
        vm_provisioner: Any,
        gitea_client: Any = None,
        container_provisioner: Any = None,
    ) -> None:
        """Initialize with dependencies.

        Args:
            db: PostgresDB instance.
            snapshot_service: SnapshotService singleton.
            vm_provisioner: VMProvisioner singleton.
            gitea_client: GiteaClient (optional, for Gitea fallback).
            container_provisioner: ContainerProvisioner (optional, for K8s IDE pods).
        """
        self._db = db
        self._workspace_recovery_store = VMWorkspaceRecoveryStore(db)
        self._snapshot_service = snapshot_service
        self._vm_provisioner = vm_provisioner
        self._container_provisioner = container_provisioner
        self._gitea_client = gitea_client
        logger.info("IDE session service initialized")

    # =========================================================================
    # Public API
    # =========================================================================

    async def get_session_status(self, job_id: str) -> dict[str, Any]:
        """Get the current IDE session status for a job.

        Combines live VM state, active session state, snapshot availability,
        and Gitea repo existence into a single status response, then withholds
        any ``code_server_url`` the contained browser transport would refuse.
        The resolution below stays intact so the advertisement returns on its
        own once the proxy guards are lifted.

        Returns:
            Dict with status, code_server_url, expires_at, etc.
        """

        return await contain_ide_status_for(
            job_id, await self._resolve_session_status(job_id)
        )

    async def _resolve_session_status(self, job_id: str) -> dict[str, Any]:
        """Resolve the status a reachable code-server would be advertised at."""

        job = await self._get_job(job_id)
        if not job:
            return {"status": "unavailable", "code_server_url": None}

        ctx = self._parse_context(job)
        vm_ctx = ctx.get("vm", {})
        session_ctx = ctx.get("ide_session", {})
        snapshot_ctx = ctx.get("snapshot", {})

        # 1a. Check for live VM (job is still processing)
        if vm_ctx.get("status") == "ready":
            ssh_host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
            if ssh_host and orchestrator_can_reach(ssh_host):
                # Routable (same-cluster) VM. The proxy targets code-server on
                # the pod IP, but the VM image ships code-server disabled until
                # the live-VM IDE lands, so only advertise a session when
                # something actually answers there — otherwise the cockpit
                # shows an "Open IDE" that 502s.
                if await self._wait_for_code_server(
                    f"http://{ssh_host}:38080", timeout=2
                ):
                    return {
                        "status": "active",
                        "code_server_url": _build_code_server_url(job_id),
                        "source": "live_vm",
                    }
                return {
                    "status": "unavailable",
                    "code_server_url": None,
                    "error": "code-server is not running on the live VM.",
                }
            if ssh_host:
                # Mesh VM (Tailscale ssh_host) the orchestrator cannot reach
                # directly; live-VM IDE via the agent tunnel is not wired yet
                # (knowledge-base/knowledge/features/vm_snapshots_and_ide.md, "Live-VM IDE Access
                # via the Agent"). Surface unavailable instead of advertising a
                # proxy URL that black-holes into a 504. Becomes "active" once
                # the agent-routed transport lands.
                return {
                    "status": "unavailable",
                    "code_server_url": None,
                    "error": "IDE is not yet available for VM-backed workspaces.",
                }

        # 1b. Check for live workspace container (job processing on container)
        ws_ctx = ctx.get("workspace_container", {})
        if ws_ctx.get("status") == "ready" and ws_ctx.get("pod_ip"):
            return {
                "status": "active",
                "code_server_url": _build_code_server_url(job_id),
                "source": "live_workspace",
            }

        # 2. Check for active IDE session
        session_status = session_ctx.get("status")
        if session_status in ("active", "idle"):
            url = session_ctx.get("code_server_url")
            expires_at = self._compute_expiry(session_ctx)
            return {
                "status": session_status,
                "code_server_url": url,
                "expires_at": expires_at,
                "started_at": session_ctx.get("started_at"),
                "restore_type": session_ctx.get("restore_type", "vm"),
                "source": session_ctx.get("source", "restored_snapshot"),
            }

        if session_status == "restoring":
            return {
                "status": "restoring",
                "code_server_url": None,
                "estimated_seconds": session_ctx.get("estimated_seconds", 30),
                "started_at": session_ctx.get("started_at"),
                "restore_type": session_ctx.get("restore_type", "vm"),
            }

        if session_status == "failed":
            return {
                "status": "failed",
                "code_server_url": None,
                "error": session_ctx.get("error"),
            }

        # Topology gate verdict is terminal: the snapshot may exist, but the
        # restore can never succeed here — surface the error instead of
        # re-offering a doomed retry (which would poll to the 5-min cap).
        if session_status == "unavailable":
            return {
                "status": "unavailable",
                "code_server_url": None,
                "error": session_ctx.get("error"),
            }

        # 3. Check for available snapshot
        if snapshot_ctx.get("status") == "available":
            return {
                "status": "available",
                "code_server_url": None,
                "snapshot_type": snapshot_ctx.get("source_type", "vm"),
            }

        # 4. Check for Gitea repo (fallback)
        if job.get("repo_name"):
            return {
                "status": "available",
                "code_server_url": None,
                "snapshot_type": "gitea",
            }

        # 5. Session was torn down (can restart)
        if session_status == "expired":
            if snapshot_ctx.get("status") == "available" or job.get("repo_name"):
                return {
                    "status": "expired",
                    "code_server_url": None,
                }

        return {"status": "unavailable", "code_server_url": None}

    async def start_session(
        self,
        job_id: str,
        cpu_cores: int = 8,
        memory: str = "16Gi",
        idle_timeout_minutes: Optional[int] = None,
    ) -> dict[str, Any]:
        """Start an IDE session for a job.

        Idempotent: returns existing session if already active/restoring.

        Args:
            job_id: Job UUID.
            cpu_cores: VM CPU cores for restored session.
            memory: VM memory for restored session.
            idle_timeout_minutes: Override default idle timeout.

        Returns:
            Session status dict.
        """
        # A restore nobody can open is pure spend — a VM or container is
        # provisioned and a snapshot pulled for a URL the proxy refuses. Refuse
        # before any of that, not after.
        contained = contained_ide_status()
        if contained is not None:
            return contained

        # Check current state first (idempotent)
        current = await self.get_session_status(job_id)
        if current["status"] in ("active", "idle"):
            return current
        if current["status"] == "unavailable":
            return {
                "status": "unavailable",
                "error": current.get("error") or "No snapshot or repo available",
            }

        job = await self._get_job(job_id)
        if not job:
            return {"status": "unavailable", "error": "Job not found"}

        ctx = self._parse_context(job)
        snapshot_ctx = ctx.get("snapshot", {})
        session_ctx = ctx.get("ide_session", {})
        snapshot_type = snapshot_ctx.get("source_type", "vm")

        if current["status"] == "restoring":
            if self._restore_task_is_active(job_id):
                return current
            if (
                session_ctx.get("restore_type") != "k8s_container"
                or not self._container_provisioner
                or not getattr(self._container_provisioner, "is_available", False)
            ):
                return current
            resumed = await self._current_ide_restore_runtime(job_id)
            if resumed is None:
                # A restoring projection without an exact settled receipt is
                # not authority to repeat external work.
                return current
            resumed_runtime, _pod_ip = resumed
            source = str(session_ctx.get("source") or "")
            if source not in {"snapshot", "gitea"}:
                return current
            snapshot_type = str(
                session_ctx.get("snapshot_type") or snapshot_type or "vm"
            )
            if source == "snapshot" and snapshot_type == "vm":
                return current
            estimated_seconds = int(session_ctx.get("estimated_seconds") or 30)
            cpu_cores = int(session_ctx.get("cpu_cores") or cpu_cores)
            memory = str(session_ctx.get("memory") or memory)
            self._schedule_restore_task(
                job_id,
                job,
                source,
                cpu_cores,
                memory,
                restore_operation_id=None,
                restore_context={"snapshot_type": snapshot_type},
                expected_restore_runtime=resumed_runtime,
            )
            return current

        # Determine restore method
        if snapshot_ctx.get("status") == "available":
            source = "snapshot"
            estimated_seconds = (
                30 if snapshot_type == "pod" else _VM_READY_TIMEOUT_SECONDS
            )
        elif job.get("repo_name"):
            source = "gitea"
            estimated_seconds = 45
        else:
            return {"status": "unavailable", "error": "No snapshot or repo available"}

        timeout = idle_timeout_minutes or self.idle_timeout_minutes
        now = datetime.now(timezone.utc).isoformat()

        restore_context = {
            "status": "restoring",
            "source": source,
            "snapshot_type": snapshot_type if source == "snapshot" else "gitea",
            "started_at": now,
            "code_server_url": None,
            "last_activity": None,
            "idle_timeout_minutes": timeout,
            "max_lifetime_minutes": self.max_lifetime_minutes,
            "estimated_seconds": estimated_seconds,
            "cpu_cores": cpu_cores,
            "memory": memory,
        }
        managed_k8s_restore = bool(
            self._container_provisioner
            and getattr(self._container_provisioner, "is_available", False)
            and (source == "gitea" or snapshot_type != "vm")
        )
        retired_runtime = None
        if (
            session_ctx.get("status") == "expired"
            and session_ctx.get("restore_type") == "k8s_container"
        ):
            retired_runtime = _canonical_runtime(
                session_ctx.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
            )
            if retired_runtime is None or not managed_k8s_restore:
                return {
                    "status": "unavailable",
                    "error": "Retired IDE runtime cannot be restored safely",
                }
        current_receipt = bool(
            managed_k8s_restore
            and _canonical_runtime(session_ctx.get(WORKSPACE_RUNTIME_INCARNATION_KEY))
            and session_ctx.get(WORKSPACE_CREATION_RESERVATION_CONTEXT_KEY)
            and session_ctx.get(WORKSPACE_CREATION_CLAIM_TOKEN_CONTEXT_KEY)
        )

        # Empty first creation retains the historical projection. A settled
        # retired A (or a retry already observing B's receipt) cannot be
        # rewritten in place: its reservation publishes B first, then a
        # runtime-guarded merge records restoring on B only.
        if retired_runtime is None and not current_receipt:
            await self._set_session_context(job_id, restore_context)

        # Start async restore (VM provisioning + snapshot extraction)
        # This runs in the background — the cockpit polls GET /ide for updates
        self._schedule_restore_task(
            job_id,
            job,
            source,
            cpu_cores,
            memory,
            restore_operation_id=retired_runtime,
            restore_context=restore_context,
            expected_restore_runtime=None,
        )

        return {
            "status": "restoring",
            "snapshot_type": snapshot_type if source == "snapshot" else "gitea",
            "estimated_seconds": estimated_seconds,
        }

    def _restore_task_is_active(self, job_id: str) -> bool:
        task = self._restore_tasks.get(job_id)
        if task is None:
            return False
        if not task.done():
            return True
        self._restore_tasks.pop(job_id, None)
        return False

    def _schedule_restore_task(
        self,
        job_id: str,
        job: dict[str, Any],
        source: str,
        cpu_cores: int,
        memory: str,
        *,
        restore_operation_id: str | None,
        restore_context: dict[str, Any] | None,
        expected_restore_runtime: str | None,
    ) -> None:
        """Start at most one local restore for a durable IDE generation."""

        if self._restore_task_is_active(job_id):
            return

        async def run() -> None:
            try:
                await self._restore_session(
                    job_id,
                    job,
                    source,
                    cpu_cores,
                    memory,
                    restore_operation_id=restore_operation_id,
                    restore_context=restore_context,
                    expected_restore_runtime=expected_restore_runtime,
                )
            finally:
                current = asyncio.current_task()
                if self._restore_tasks.get(job_id) is current:
                    self._restore_tasks.pop(job_id, None)

        self._restore_tasks[job_id] = asyncio.create_task(run())

    async def stop_session(self, job_id: str) -> dict[str, Any]:
        """Manually tear down an active IDE session.

        Returns:
            Status dict.
        """
        job = await self._get_job(job_id)
        if not job:
            return {"status": "not_found"}

        ctx = self._parse_context(job)
        session_ctx = ctx.get("ide_session", {})
        status = session_ctx.get("status")

        if status not in ("active", "idle", "restoring", "cleanup_pending", "failed"):
            return {"status": "no_active_session"}

        restore_type = session_ctx.get("restore_type", "vm")
        expected_runtime_incarnation = session_ctx.get("_runtime_incarnation")
        stale_target_settled = False

        if restore_type in ("container", "k8s_container"):
            container_name = session_ctx.get("container_name")
            if restore_type == "k8s_container" and container_name:
                outcome = await self._delete_k8s_ide_container_with_outcome(
                    job_id,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
                deleted = outcome.current_deleted
                stale_target_settled = outcome.stale_target_settled
            else:
                deleted = bool(
                    container_name
                    and await self._delete_ide_container(
                        job_id,
                        container_name,
                        restore_type,
                        expected_container_id=session_ctx.get("container_id"),
                        expected_runtime_incarnation=expected_runtime_incarnation,
                    )
                )
        else:
            vm_name = session_ctx.get("vm_name")
            deleted = False
            if vm_name and self._vm_provisioner:
                try:
                    deleted = await self._delete_ide_vm(job_id, vm_name)
                except Exception as e:
                    logger.warning("Failed to delete IDE VM %s: %s", vm_name, e)
        if stale_target_settled:
            return {
                "status": "superseded",
                "job_id": job_id,
                "retryable": False,
            }
        if not deleted:
            updates = {
                "status": "cleanup_pending",
                "code_server_url": None,
                "cleanup_failure": "managed_repository_process_zero_unproven",
            }
            if restore_type == "k8s_container":
                if not await self._set_session_context_if_runtime(
                    job_id,
                    updates,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                ):
                    return {
                        "status": "superseded",
                        "job_id": job_id,
                        "retryable": False,
                    }
            else:
                await self._set_session_context(job_id, updates)
            return {"status": "cleanup_pending", "job_id": job_id, "retryable": True}

        updates = {
            "status": "expired",
            "code_server_url": None,
            "stopped_at": datetime.now(timezone.utc).isoformat(),
        }
        if restore_type == "k8s_container":
            if not await self._set_session_context_if_runtime(
                job_id,
                updates,
                expected_runtime_incarnation=expected_runtime_incarnation,
            ):
                return {
                    "status": "superseded",
                    "job_id": job_id,
                    "retryable": False,
                }
        else:
            await self._set_session_context(job_id, updates)

        return {"status": "stopped", "job_id": job_id}

    async def check_ttl_all(self) -> int:
        """Check all active IDE sessions and expire those past TTL.

        Called periodically by the background sweeper task.

        Returns:
            Number of sessions expired.
        """
        if not self._db:
            return 0

        expired_count = 0
        try:
            # Query jobs with active IDE sessions
            async with self._db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, context
                    FROM jobs
                    WHERE context->'ide_session'->>'status'
                          IN ('active', 'idle', 'cleanup_pending')
                    """
                )

            now = datetime.now(timezone.utc)
            for row in rows:
                job_id = str(row["id"])
                ctx = (
                    row["context"]
                    if isinstance(row["context"], dict)
                    else json.loads(row["context"] or "{}")
                )
                session = ctx.get("ide_session", {})

                should_expire = False
                reason = ""

                # Check max lifetime
                started_at = session.get("started_at")
                max_lifetime = session.get(
                    "max_lifetime_minutes", self.max_lifetime_minutes
                )
                if started_at:
                    start = datetime.fromisoformat(started_at)
                    if now - start > timedelta(minutes=max_lifetime):
                        should_expire = True
                        reason = "max_lifetime"

                # Check idle timeout (only for "idle" status)
                if not should_expire and session.get("status") == "idle":
                    last_activity = session.get("last_activity")
                    idle_timeout = session.get(
                        "idle_timeout_minutes", self.idle_timeout_minutes
                    )
                    if last_activity:
                        last = datetime.fromisoformat(last_activity)
                        if now - last > timedelta(minutes=idle_timeout):
                            should_expire = True
                            reason = "idle_timeout"
                    elif started_at:
                        # No activity recorded since start — use started_at as baseline
                        start = datetime.fromisoformat(started_at)
                        if now - start > timedelta(minutes=idle_timeout):
                            should_expire = True
                            reason = "idle_timeout_no_activity"

                if should_expire:
                    logger.info(
                        "Expiring IDE session for job %s (reason: %s)", job_id, reason
                    )
                    result = await self.stop_session(job_id)
                    if result.get("status") == "stopped":
                        expired_count += 1

        except Exception:
            logger.exception("Error during IDE session TTL check")

        return expired_count

    # =========================================================================
    # Restore logic (runs as background task)
    # =========================================================================

    async def _current_ide_restore_runtime(
        self,
        job_id: str,
        *,
        creation: dict[str, Any] | None = None,
        retired_runtime: str | None = None,
    ) -> tuple[str, str] | None:
        """Read B from its exact settled creation receipt and owner projection."""

        if not self._container_provisioner:
            return None
        if creation is None:
            creation = (
                await self._container_provisioner.get_current_ide_creation_result(
                    job_id
                )
            )
        current = await self._get_job(job_id)
        if not isinstance(current, dict):
            return None
        session = self._parse_context(current).get("ide_session", {})
        runtime = _settled_ide_restore_runtime(
            session,
            creation,
            retired_runtime=retired_runtime,
        )
        pod_ip = session.get("pod_ip") if isinstance(session, dict) else None
        if runtime is None or not isinstance(pod_ip, str) or not pod_ip:
            return None
        return runtime, pod_ip

    async def _exact_ide_restore_runtime(
        self,
        job_id: str,
        *,
        operation_id: str | None,
        expected_runtime: str | None,
    ) -> tuple[str, str] | None:
        """Resolve B without accepting a later receipt-bearing successor C."""

        if not self._container_provisioner:
            return None
        creation = None
        if operation_id is not None:
            creation = await self._container_provisioner.get_ide_creation_result(
                job_id,
                operation_id=operation_id,
            )
        elif expected_runtime is None:
            return None
        current = await self._current_ide_restore_runtime(
            job_id,
            creation=creation,
            retired_runtime=operation_id,
        )
        if current is None:
            return None
        runtime, _pod_ip = current
        if expected_runtime is not None and runtime != expected_runtime:
            return None
        return current

    async def _claim_ide_restore_work(
        self,
        job_id: str,
        *,
        runtime_incarnation: str,
    ) -> tuple[str, dict[str, Any]] | None:
        """Claim B's external restore tail, replaying a lost claim response."""

        claimant = f"ide-restore:{uuid4()}"
        for _attempt in range(2):
            try:
                restore_work = await self._container_provisioner.claim_ide_restore_work(
                    job_id,
                    claimant=claimant,
                    lease_seconds=300,
                )
            except Exception:
                logger.warning(
                    "IDE restore-work claim response was ambiguous for job %s",
                    job_id,
                )
                continue
            if _claimed_ide_restore_work(
                restore_work,
                runtime_incarnation=runtime_incarnation,
                claimant=claimant,
            ):
                return claimant, restore_work
            return None
        return None

    def _ide_restore_heartbeat(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> RestoreWorkLeaseHeartbeat:
        async def renew() -> object:
            return await self._container_provisioner.renew_ide_restore_work(
                job_id,
                restore_work=restore_work,
                claimant=claimant,
                lease_seconds=300,
            )

        return RestoreWorkLeaseHeartbeat(renew, interval_seconds=60)

    async def _attest_claimed_ide_restore_target(
        self,
        job_id: str,
        *,
        runtime_incarnation: str,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> WorkspaceRuntimeAttestation:
        """Attest exact B, then re-lock and validate its work token.

        Kubernetes Pod IPs are reusable. The control-plane UID/endpoint/key
        observation is therefore followed immediately by a durable renewal;
        no byte or repository key may leave this process unless both views
        still name the same B generation.
        """

        attestation = await self._container_provisioner.attest_ide_runtime(
            job_id,
            expected_runtime_incarnation=runtime_incarnation,
        )
        if attestation.runtime_incarnation != runtime_incarnation:
            raise WorkspaceRuntimeAuthorityError("IDE restore Pod UID changed")
        renewed = await self._container_provisioner.renew_ide_restore_work(
            job_id,
            restore_work=restore_work,
            claimant=claimant,
            lease_seconds=300,
        )
        if not _claimed_ide_restore_work(
            renewed,
            runtime_incarnation=runtime_incarnation,
            claimant=claimant,
        ) or any(
            str(renewed.get(key) or "") != str(restore_work.get(key) or "")
            for key in ("id", "restore_work_claim_token")
        ):
            raise RestoreWorkLeaseLost(
                "IDE restore authority changed after runtime attestation"
            )
        return attestation

    async def _release_ide_restore_work(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
    ) -> None:
        try:
            await self._container_provisioner.release_ide_restore_work(
                job_id,
                restore_work=restore_work,
                claimant=claimant,
                retry_seconds=30,
            )
        except Exception:
            # The bounded lease remains the restart/reclaim authority.
            logger.warning(
                "IDE restore-work release response was ambiguous for job %s",
                job_id,
            )

    async def _complete_ide_restore_work(
        self,
        job_id: str,
        *,
        restore_work: dict[str, Any],
        claimant: str,
        success: bool,
        error: str | None = None,
    ) -> bool:
        """Atomically publish exact B, replaying one lost completion response."""

        code_server_url = _build_code_server_url(job_id) if success else None
        last_activity = datetime.now(timezone.utc).isoformat() if success else None
        for _attempt in range(2):
            try:
                if await self._container_provisioner.complete_ide_restore_work(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                    success=success,
                    code_server_url=code_server_url,
                    last_activity=last_activity,
                    error=error,
                ):
                    return True
            except Exception:
                logger.warning(
                    "IDE restore-work completion response was ambiguous for job %s",
                    job_id,
                )
        return False

    async def _create_or_resume_k8s_ide(
        self,
        job_id: str,
        *,
        operation_id: str | None,
    ) -> tuple[str, str] | None:
        """Create B, or resume the exact B after a committed/lost response."""

        if not self._container_provisioner:
            return None
        if operation_id is None:
            current = await self._current_ide_restore_runtime(job_id)
            if current is not None:
                return current
        try:
            if operation_id is None:
                await self._container_provisioner.create_ide_pod(job_id)
            else:
                await self._container_provisioner.create_ide_pod(
                    job_id,
                    operation_id=operation_id,
                )
        except Exception:
            logger.warning(
                "IDE creation response was ambiguous for job %s",
                job_id,
                exc_info=True,
            )
        if operation_id is None:
            creation = (
                await self._container_provisioner.get_current_ide_creation_result(
                    job_id
                )
            )
        else:
            creation = await self._container_provisioner.get_ide_creation_result(
                job_id,
                operation_id=operation_id,
            )
        return await self._current_ide_restore_runtime(
            job_id,
            creation=creation,
            retired_runtime=operation_id,
        )

    async def _restore_session(
        self,
        job_id: str,
        job: dict,
        source: str,
        cpu_cores: int,
        memory: str,
        *,
        restore_operation_id: str | None = None,
        restore_context: dict[str, Any] | None = None,
        expected_restore_runtime: str | None = None,
    ) -> None:
        """Background task: provision environment and start code-server.

        Two paths:
        - snapshot/vm: Provision KubeVirt VM → extract S3 snapshot → code-server
        - gitea: Spin up lightweight code-server container → git clone

        Updates context.ide_session as the restore progresses.
        """
        k8s_restore_kwargs: dict[str, Any] = {}
        managed_restore_claim: tuple[str, dict[str, Any]] | None = None
        managed_restore_finished = False
        snapshot_type = str(
            (restore_context or {}).get("snapshot_type")
            or self._parse_context(job).get("snapshot", {}).get("source_type", "vm")
        )
        managed_k8s_restore = bool(
            self._container_provisioner
            and getattr(self._container_provisioner, "is_available", False)
            and (source == "gitea" or snapshot_type != "vm")
        )
        try:
            if managed_k8s_restore:
                restored = await self._create_or_resume_k8s_ide(
                    job_id,
                    operation_id=restore_operation_id,
                )
                if restored is None:
                    return
                runtime, _pod_ip = restored
                managed_restore_claim = await self._claim_ide_restore_work(
                    job_id,
                    runtime_incarnation=runtime,
                )
                if managed_restore_claim is None:
                    # Another replica owns the exact-B work lease. This local
                    # task exits; retry/restart observes the same durable B.
                    return
                k8s_restore_kwargs = {
                    "restore_operation_id": restore_operation_id,
                    "restore_context": restore_context,
                    "restored": restored,
                    "restore_claim": managed_restore_claim,
                    "settle_restore_work": False,
                }

            restored_ok = False
            if source == "gitea":
                # Lightweight container path — no VM needed
                restored_ok = await self._restore_gitea_container(
                    job_id, job, **k8s_restore_kwargs
                )
            elif source == "snapshot":
                if snapshot_type == "vm":
                    # Legacy VM snapshots remain in the VM restore flow
                    await self._restore_vm_session(
                        job_id, job, source, cpu_cores, memory
                    )
                    return

                restored_ok = False
                if (
                    self._container_provisioner
                    and self._container_provisioner.is_available
                ):
                    restored_ok = await self._restore_snapshot_container(
                        job_id, job, **k8s_restore_kwargs
                    )

                if not restored_ok:
                    if not job.get("repo_name"):
                        restored_ok = False
                    else:
                        logger.warning(
                            "Pod snapshot restore failed for job %s; falling back to Gitea clone",
                            job_id,
                        )
                        # The fallback runs under the same exact-B work token.
                        # No failed projection or retry delay is inserted
                        # between the two external strategies.
                        restored_ok = await self._restore_gitea_container(
                            job_id, job, **k8s_restore_kwargs
                        )

            else:
                # Full VM path — provision VM, extract snapshot
                await self._restore_vm_session(job_id, job, source, cpu_cores, memory)
                return

            if managed_restore_claim is not None:
                # Snapshot, repository and code-server failures are external
                # and retryable. Only a proven successful tail is absorbing;
                # otherwise release the exact-B lease so another request can
                # resume the same generation after backoff. Marking every
                # transient failure ``failed`` would permanently close the
                # creation receipt and strand the durable restoring runtime.
                if restored_ok:
                    claimant, restore_work = managed_restore_claim
                    managed_restore_finished = await self._complete_ide_restore_work(
                        job_id,
                        restore_work=restore_work,
                        claimant=claimant,
                        success=True,
                    )

        except Exception as e:
            logger.exception("IDE session restore failed for job %s", job_id)
            if managed_restore_claim is not None:
                # Leave B in restoring and release its lease in ``finally``.
                # The durable receipt plus current-runtime markers let a
                # process restart retry without rebuilding or mutating A/C.
                return
            current_runtime = None
            if k8s_restore_kwargs:
                current_runtime = await self._exact_ide_restore_runtime(
                    job_id,
                    operation_id=restore_operation_id,
                    expected_runtime=expected_restore_runtime,
                )
            if current_runtime is None:
                if not k8s_restore_kwargs:
                    await self._set_session_context(
                        job_id,
                        {"status": "failed", "error": str(e)},
                    )
            else:
                runtime, _pod_ip = current_runtime
                await self._set_session_context_if_runtime(
                    job_id,
                    {"status": "failed", "error": str(e)},
                    expected_runtime_incarnation=runtime,
                )
        finally:
            if managed_restore_claim is not None and not managed_restore_finished:
                claimant, restore_work = managed_restore_claim
                await self._release_ide_restore_work(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                )

    async def _restore_vm_session(
        self,
        job_id: str,
        job: dict,
        source: str,
        cpu_cores: int,
        memory: str,
    ) -> None:
        """Restore an IDE session via a full KubeVirt VM."""
        if not self._vm_provisioner or not self._vm_provisioner.is_available:
            await self._set_session_context(
                job_id,
                {
                    "status": "failed",
                    "error": "VM provisioner not available",
                },
            )
            return

        # Temporary security containment.  VM restore currently has no
        # durable operation receipt that can CAS the final ide_session
        # projection to the same provision generation after snapshot/Git/
        # profile writes.  A one-shot endpoint read would let a replacement VM
        # at the same address receive archive bytes or a repository key, and a
        # post-check alone cannot make that final publication atomic.  Keep the
        # separately supported live-VM observation/proxy path available, but do
        # not create or mutate a restore VM until that generation-CAS seam
        # exists.
        await self._set_session_context(
            job_id,
            {
                "status": "unavailable",
                "error": "VM IDE restore awaits exact generation authority",
            },
        )

    async def _restore_gitea_container(
        self,
        job_id: str,
        job: dict,
        *,
        restore_operation_id: str | None = None,
        restore_context: dict[str, Any] | None = None,
        restored: tuple[str, str] | None = None,
        restore_claim: tuple[str, dict[str, Any]] | None = None,
        settle_restore_work: bool = True,
    ) -> bool:
        """Restore an IDE session via a lightweight code-server container.

        Spins up a code-server container, clones the Gitea repo into it,
        and returns the code-server URL. Much faster than a full VM (~15s
        vs ~45s) but provides only the workspace files, not the full
        agent environment.

        For production (K8s): creates a Pod via ContainerProvisioner.
        For local dev: falls back to podman/docker subprocess.
        """
        repo_name = job.get("repo_name")
        branch = job.get("branch_name") or "main"
        if not repo_name:
            if restore_claim is not None:
                # The receipt-backed caller owns retry/settlement. Do not
                # bypass it with a standalone context verdict.
                return False
            failure = {
                "status": "failed",
                "error": "No Gitea repo available",
            }
            if restored is None:
                await self._set_session_context(job_id, failure)
            else:
                await self._set_session_context_if_runtime(
                    job_id,
                    failure,
                    expected_runtime_incarnation=restored[0],
                )
            return False

        # Route to K8s or local container path
        if self._container_provisioner and self._container_provisioner.is_available:
            return await self._restore_k8s_ide_container(
                job_id,
                job,
                repo_name,
                branch,
                restore_operation_id=restore_operation_id,
                restore_context=restore_context,
                restored=restored,
                restore_claim=restore_claim,
                settle_restore_work=settle_restore_work,
            )
        else:
            return await self._restore_local_ide_container(job_id, repo_name, branch)

    async def _managed_repository_payload(
        self, job_id: str, *, backend: str
    ) -> dict[str, Any]:
        """Return one exact internal deploy-key bundle for an IDE workspace.

        IDE restore is a second workspace ingress, independent of the normal
        dispatcher. It therefore resolves repository authority from the
        current job row rather than rebuilding an administrator URL from
        environment variables. The caller immediately removes the private
        key from this mapping and sends it only over stdin to the target.
        """

        if (
            not self._db
            or not self._gitea_client
            or not self._gitea_client.is_initialized
        ):
            raise ManagedRepositoryAuthorityError("repository_authority_unavailable")
        current = await self._get_job(job_id)
        if current is None or not current.get("repo_name"):
            raise ManagedRepositoryAuthorityError("repository_authority_unavailable")
        _url, _repositories, payloads = await authorize_job_repository_transport(
            self._db,
            self._gitea_client,
            current,
            None,
            backend=backend,
        )
        if len(payloads) != 1:
            raise ManagedRepositoryAuthorityError("repository_authority_ambiguous")
        if not await self._db.managed_repository_authorities_are_current(payloads):
            raise ManagedRepositoryAuthorityError("repository_authority_raced")
        return payloads[0]

    @staticmethod
    def _managed_git_command(
        payload: dict[str, Any],
        *,
        branch: str,
        workspace_path: str,
        require_existing: bool,
    ) -> tuple[str, bytearray]:
        """Build a secret-free shell command plus its stdin-only private key."""

        private_text = payload.pop("private_key", None)
        if not isinstance(private_text, str) or not private_text:
            raise ManagedRepositoryAuthorityError("repository_authority_unavailable")
        private_key = bytearray(private_text.encode("utf-8"))
        del private_text
        try:
            version = int(payload.get("version") or 0)
            generation = int(payload.get("generation") or 0)
            port = int(payload.get("ssh_port") or 0)
        except (TypeError, ValueError) as exc:
            for index in range(len(private_key)):
                private_key[index] = 0
            raise ManagedRepositoryAuthorityError(
                "repository_authority_invalid"
            ) from exc
        authority_id = str(payload.get("authority_id") or "").replace("-", "")
        access_mode = str(payload.get("access_mode") or "")
        repo_name = str(payload.get("repo_name") or "")
        repository_owner = str(payload.get("repository_owner") or "")
        alias = str(payload.get("alias") or "")
        host = str(payload.get("ssh_host") or "")
        clone_url = str(payload.get("clone_url") or "")
        fingerprint = str(payload.get("public_key_fingerprint") or "")
        # Values originate in the validated authority service, but keep this
        # shell boundary independently strict so operator configuration cannot
        # become command/config injection.
        import re
        from urllib.parse import urlparse

        parsed_clone_url = urlparse(clone_url)

        if (
            version != 1
            or generation < 1
            or not re.fullmatch(r"[a-f0-9]{32}", authority_id)
            or access_mode not in {"read", "write"}
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", repo_name)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", repository_owner)
            or not re.fullmatch(r"srw-repo-[a-f0-9]{32}", alias)
            or alias != f"srw-repo-{authority_id}"
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,252}", host)
            or not 1 <= port <= 65535
            or parsed_clone_url.scheme != "ssh"
            or parsed_clone_url.hostname != alias
            or parsed_clone_url.username is not None
            or parsed_clone_url.password is not None
            or parsed_clone_url.port is not None
            or parsed_clone_url.path != f"/{repository_owner}/{repo_name}.git"
            or not re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", fingerprint)
            or not branch
            or "\x00" in branch
        ):
            for index in range(len(private_key)):
                private_key[index] = 0
            raise ManagedRepositoryAuthorityError("repository_authority_invalid")

        home_path = workspace_path.removesuffix("/workspace")
        if home_path not in {"/home/agent-host", "/home/coder"}:
            for index in range(len(private_key)):
                private_key[index] = 0
            raise ManagedRepositoryAuthorityError("repository_authority_invalid")
        managed_root = f"{home_path}/.ssh/srw-managed"
        socket_path = f"{managed_root}/sockets/{authority_id}.sock"
        ssh_config_path = f"{home_path}/.ssh/config"
        config = (
            f"Host {alias}\n"
            f"  HostName {host}\n"
            f"  Port {port}\n"
            "  User git\n"
            f"  IdentityAgent {socket_path}\n"
            # The dedicated agent contains exactly this authority's key. Do
            # not set IdentitiesOnly without an IdentityFile: OpenSSH would
            # suppress the agent-only identity we intentionally keep off disk.
            "  BatchMode yes\n"
            "  StrictHostKeyChecking accept-new\n"
            f"  UserKnownHostsFile {managed_root}/known_hosts\n"
        )
        quoted_workspace = shlex.quote(workspace_path)
        quoted_url = shlex.quote(clone_url)
        quoted_branch = shlex.quote(branch)
        existing_clause = (
            f"test -d {quoted_workspace}/.git; " if require_existing else ""
        )
        launch_agent = managed_repository_agent_launch_command(
            home_path=home_path,
            authority_id=authority_id,
            generation=generation,
            preserve_existing=True,
            keep_rollback_trap=True,
            expected_fingerprint=fingerprint,
            probe_url=clone_url,
            config_content=config,
        )
        clone_clause = (
            f"if test -d {quoted_workspace}/.git; then "
            f"git -C {quoted_workspace} remote set-url origin {quoted_url}; "
            f"git -C {quoted_workspace} fetch origin {quoted_branch}; "
            "else "
            f'test -z "$(ls -A {quoted_workspace} 2>/dev/null)"; '
            f"git clone --branch {quoted_branch} {quoted_url} {quoted_workspace}; "
            "fi"
        )
        command = (
            "set -eu; umask 077; "
            f"mkdir -p {shlex.quote(managed_root + '/config.d')} "
            f"{shlex.quote(managed_root + '/sockets')} "
            f"{shlex.quote(managed_root + '/agents')}; "
            f"exec 8>{shlex.quote(managed_root + '/setup.lock')}; flock -x 8; "
            f"touch {shlex.quote(ssh_config_path)}; "
            "grep -qxF 'Include ~/.ssh/srw-managed/config.d/*.conf' "
            f"{shlex.quote(ssh_config_path)} || "
            "printf '\\nInclude ~/.ssh/srw-managed/config.d/*.conf\\n' "
            f">> {shlex.quote(ssh_config_path)}; "
            f"chmod 600 {shlex.quote(ssh_config_path)}; "
            + launch_agent
            + "; "
            + f"mkdir -p {quoted_workspace}; "
            + existing_clause
            + clone_clause
            + "; "
            + f'case "$(git -C {quoted_workspace} remote get-url origin)" '
            + "in *://*@*|*@*:*) exit 41;; esac; trap - EXIT"
        )
        return command, private_key

    @staticmethod
    async def _run_secret_stdin_process(
        command: list[str], secret: bytearray, *, timeout: float = 120.0
    ) -> bool:
        """Run a trusted command without retaining or reporting its output."""

        import asyncio

        process = None
        try:
            process = await create_owned_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )

            async def _complete() -> bool:
                async with asyncio.timeout(timeout):
                    if process.stdin is None:
                        return False
                    process.stdin.write(secret)
                    await process.stdin.drain()
                    process.stdin.close()
                    secret[:] = b"\x00" * len(secret)
                    await process.wait()
                    return process.returncode == 0

            return bool(await joined_async_call(_complete()))
        except asyncio.CancelledError:
            # Durable restore-work lease loss cancels the owner task. Reap the
            # SSH transport before returning the token so no old replica keeps
            # a repository-key operation alive against B (or an IP successor).
            if process is not None and process.returncode is None:
                await stop_and_reap(process)
            raise
        except (OSError, asyncio.TimeoutError):
            if process is not None and process.returncode is None:
                await stop_and_reap(process)
            return False
        finally:
            for index in range(len(secret)):
                secret[index] = 0

    async def _install_and_sync_managed_repository_over_ssh(
        self,
        job_id: str,
        *,
        backend: str,
        ssh_host: str,
        ssh_port: int,
        branch: str,
        workspace_path: str,
        require_existing: bool = False,
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> bool:
        """Install repo-scoped authority into an IDE pod/VM and clone/fetch."""

        # A raw coordinate is never recipient authority for a repository key.
        # Every remote caller must supply both a pinned SSH identity and a
        # fresh runtime/lease callback; refuse before even loading key bytes.
        if expected_host_key_fingerprint is None or mutation_authority is None:
            return False
        try:
            payload = await self._managed_repository_payload(job_id, backend=backend)
            command, private_key = self._managed_git_command(
                payload,
                branch=branch,
                workspace_path=workspace_path,
                require_existing=require_existing,
            )
            try:
                attestation = await mutation_authority()
                ssh_host = attestation.pod_ip
                ssh_port = attestation.port
                expected_host_key_fingerprint = attestation.ssh_host_key_fingerprint
                async with pinned_agent_ssh_command(
                    ssh_host,
                    ssh_port,
                    command,
                    expected_host_key_fingerprint=(expected_host_key_fingerprint),
                ) as ssh_command:
                    return await self._run_secret_stdin_process(
                        ssh_command, private_key
                    )
            finally:
                private_key[:] = b"\x00" * len(private_key)
        except (ManagedRepositoryAuthorityError, SSHHostKeyVerificationError):
            return False

    async def _restore_k8s_ide_container(
        self,
        job_id: str,
        job: dict,
        repo_name: str,
        branch: str,
        *,
        restore_operation_id: str | None = None,
        restore_context: dict[str, Any] | None = None,
        restored: tuple[str, str] | None = None,
        restore_claim: tuple[str, dict[str, Any]] | None = None,
        settle_restore_work: bool = True,
    ) -> bool:
        """Create an IDE pod on Kubernetes, clone the repo, return code-server URL."""
        pod_name = f"ide-{job_id[:12]}"

        if restored is None:
            restored = await self._create_or_resume_k8s_ide(
                job_id,
                operation_id=restore_operation_id,
            )
        if restored is None:
            return False
        runtime, pod_ip = restored
        owns_claim = restore_claim is None
        if restore_claim is None:
            restore_claim = await self._claim_ide_restore_work(
                job_id,
                runtime_incarnation=runtime,
            )
        if restore_claim is None:
            return False
        claimant, restore_work = restore_claim
        restore_work_finished = False
        effects_ok = False
        try:
            try:
                async with self._ide_restore_heartbeat(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                ):
                    attestation = await self._attest_claimed_ide_restore_target(
                        job_id,
                        runtime_incarnation=runtime,
                        restore_work=restore_work,
                        claimant=claimant,
                    )
                    pod_ip = attestation.pod_ip
                    host_fingerprint = attestation.ssh_host_key_fingerprint

                    async def mutation_authority() -> WorkspaceRuntimeAttestation:
                        return await self._attest_claimed_ide_restore_target(
                            job_id,
                            runtime_incarnation=runtime,
                            restore_work=restore_work,
                            claimant=claimant,
                        )

                    initial = {
                        **(restore_context or {}),
                        "status": "restoring",
                        "error": None,
                        "container_name": pod_name,
                        "restore_type": "k8s_container",
                    }
                    if not await self._set_session_context_if_runtime(
                        job_id,
                        initial,
                        expected_runtime_incarnation=runtime,
                    ):
                        return False

                    # Clone through the exact repository deploy key. No Gitea
                    # credential, URL userinfo, or private-key file enters the
                    # IDE workspace.
                    installed = (
                        await self._install_and_sync_managed_repository_over_ssh(
                            job_id,
                            backend="sandbox",
                            ssh_host=pod_ip,
                            ssh_port=attestation.port,
                            branch=branch,
                            workspace_path="/home/agent-host/workspace",
                            expected_host_key_fingerprint=host_fingerprint,
                            mutation_authority=mutation_authority,
                        )
                    )
                    if installed:
                        # code-server is already running from the workspace
                        # entrypoint. Its health check remains inside the
                        # renewable lease because it may consume the whole
                        # bounded timeout.
                        ready = await self._wait_for_code_server(
                            f"http://{pod_ip}:38080", timeout=15
                        )
                        if not ready:
                            logger.warning(
                                "code-server not responding on IDE pod %s — "
                                "setting active anyway",
                                pod_name,
                            )
                        post_attestation = await mutation_authority()
                        effects_ok = post_attestation == attestation and (
                            await self._container_provisioner.ide_pod_live(
                                job_id,
                                expected_runtime_incarnation=runtime,
                            )
                            is True
                        )
                    else:
                        logger.warning(
                            "Scoped Git setup failed for IDE session job %s", job_id
                        )
            except (RestoreWorkLeaseLost, WorkspaceRuntimeAuthorityError):
                logger.warning("IDE restore-work authority changed for job %s", job_id)
                return False

            if not settle_restore_work:
                return effects_ok
            if not effects_ok:
                # Repository and readiness failures are retryable. Releasing
                # the lease preserves exact B; deleting its Pod after the
                # creation receipt settled would make the same generation
                # impossible to resume.
                return False
            if not await self._complete_ide_restore_work(
                job_id,
                restore_work=restore_work,
                claimant=claimant,
                success=True,
            ):
                return False
            restore_work_finished = True

            logger.info(
                "IDE session active (K8s container) for job %s: %s",
                job_id,
                _build_code_server_url(job_id),
            )
            return True
        finally:
            if owns_claim and not restore_work_finished:
                await self._release_ide_restore_work(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                )

    async def _restore_local_ide_container(
        self, job_id: str, repo_name: str, branch: str
    ) -> bool:
        """Fallback: run code-server via local podman/docker (dev environments)."""
        import asyncio

        container_name = f"srw-ide-{job_id[:12]}"
        host_port = await self._find_free_port()

        code_server_image = os.environ.get(
            "CODE_SERVER_IMAGE", "docker.io/codercom/code-server:latest"
        )

        await self._set_session_context(
            job_id,
            {
                "container_name": container_name,
                "restore_type": "container",
            },
        )

        try:
            runtime = await self._detect_container_runtime()

            entrypoint_script = (
                "mkdir -p /home/coder/workspace && "
                f"exec code-server --bind-addr 0.0.0.0:{host_port} "
                "--auth none /home/coder/workspace"
            )

            cmd = [
                runtime,
                "run",
                "-d",
                "--name",
                container_name,
                "--network",
                "host",
                "-e",
                f"PORT={host_port}",
                "--label",
                f"srw.job_id={job_id}",
                "--label",
                "srw.type=ide-session",
                "--entrypoint",
                "sh",
                code_server_image,
                "-c",
                entrypoint_script,
            ]

            proc = await create_owned_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await communicate_bounded(
                proc,
                timeout=120,
                stdout_limit=4096,
                stderr_limit=64 * 1024,
            )

            if proc.returncode != 0:
                error_msg = stderr.decode(errors="replace")[:500]
                logger.error(
                    "Failed to start IDE container for job %s: %s",
                    job_id,
                    error_msg,
                )
                await self._set_session_context(
                    job_id,
                    {
                        "status": "failed",
                        "error": f"Container start failed: {error_msg}",
                    },
                )
                return False
            container_id = stdout.decode(errors="replace").strip()
            if re.fullmatch(r"[0-9a-f]{64}", container_id) is None:
                await self._set_session_context(
                    job_id,
                    {
                        "status": "failed",
                        "error": "Container identity was not reported",
                    },
                )
                return False
            await self._set_session_context(job_id, {"container_id": container_id})

            try:
                # The local runtime name is mutable.  Re-resolve it only as a
                # consistency assertion, then deliver the private authority to
                # the immutable full container ID captured from ``run``.  If a
                # replacement wins this window the exact-ID exec fails instead
                # of handing its key to the replacement.
                observed_container_id = await self._inspect_container_id(
                    runtime, container_name
                )
                if observed_container_id != container_id:
                    installed = False
                else:
                    payload = await self._managed_repository_payload(
                        job_id, backend="sandbox"
                    )
                    git_command, private_key = self._managed_git_command(
                        payload,
                        branch=branch,
                        workspace_path="/home/coder/workspace",
                        require_existing=False,
                    )
                    installed = await self._run_secret_stdin_process(
                        [
                            runtime,
                            "exec",
                            "-i",
                            container_id,
                            "sh",
                            "-c",
                            git_command,
                        ],
                        private_key,
                    )
            except ManagedRepositoryAuthorityError:
                installed = False
            if not installed:
                await self._set_session_context(
                    job_id,
                    {
                        "status": "failed",
                        "error": "Repository authorization failed for IDE session",
                    },
                )
                await self._delete_ide_container(
                    job_id,
                    container_name,
                    "container",
                    expected_container_id=container_id,
                )
                return False

            ready = await self._wait_for_code_server(
                f"http://localhost:{host_port}", timeout=30
            )
            if not ready:
                await self._set_session_context(
                    job_id,
                    {
                        "status": "failed",
                        "error": "Code-server did not start within timeout",
                    },
                )
                await self._delete_ide_container(
                    job_id,
                    container_name,
                    "container",
                    expected_container_id=container_id,
                )
                return False

            code_server_url = (
                f"http://localhost:{host_port}/?folder=/home/coder/workspace"
            )

            await self._set_session_context(
                job_id,
                {
                    "status": "active",
                    "code_server_url": code_server_url,
                    "restore_type": "container",
                    "host_port": host_port,
                    "last_activity": datetime.now(timezone.utc).isoformat(),
                },
            )

            logger.info(
                "IDE session active (container) for job %s: %s",
                job_id,
                code_server_url,
            )
            return True

        except Exception as e:
            logger.exception("Container IDE session failed for job %s", job_id)
            await self._set_session_context(
                job_id,
                {
                    "status": "failed",
                    "error": str(e),
                },
            )
            try:
                runtime = await self._detect_container_runtime()
                current = await self._get_job(job_id)
                current_ide = (
                    self._parse_context(current).get("ide_session", {})
                    if current
                    else {}
                )
                await self._delete_ide_container(
                    job_id,
                    container_name,
                    "container",
                    expected_container_id=current_ide.get("container_id"),
                )
            except Exception:
                pass
            return False

    async def _restore_snapshot_container(
        self,
        job_id: str,
        job: dict,
        *,
        restore_operation_id: str | None = None,
        restore_context: dict[str, Any] | None = None,
        restored: tuple[str, str] | None = None,
        restore_claim: tuple[str, dict[str, Any]] | None = None,
        settle_restore_work: bool = True,
    ) -> bool:
        """Restore an IDE session via in-cluster pod from an in-cluster snapshot."""
        pod_name = f"ide-{job_id[:12]}"

        if not self._container_provisioner or not getattr(
            self._container_provisioner, "is_available", True
        ):
            return False

        if restored is None:
            restored = await self._create_or_resume_k8s_ide(
                job_id,
                operation_id=restore_operation_id,
            )
        if restored is None:
            return False
        runtime, pod_ip = restored
        owns_claim = restore_claim is None
        if restore_claim is None:
            restore_claim = await self._claim_ide_restore_work(
                job_id,
                runtime_incarnation=runtime,
            )
        if restore_claim is None:
            return False
        claimant, restore_work = restore_claim
        restore_work_finished = False
        effects_ok = False
        try:
            try:
                async with self._ide_restore_heartbeat(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                ):
                    attestation = await self._attest_claimed_ide_restore_target(
                        job_id,
                        runtime_incarnation=runtime,
                        restore_work=restore_work,
                        claimant=claimant,
                    )
                    pod_ip = attestation.pod_ip
                    host_fingerprint = attestation.ssh_host_key_fingerprint

                    async def mutation_authority() -> WorkspaceRuntimeAttestation:
                        return await self._attest_claimed_ide_restore_target(
                            job_id,
                            runtime_incarnation=runtime,
                            restore_work=restore_work,
                            claimant=claimant,
                        )

                    if not await self._set_session_context_if_runtime(
                        job_id,
                        {
                            **(restore_context or {}),
                            "status": "restoring",
                            "error": None,
                            "container_name": pod_name,
                            "restore_type": "k8s_container",
                        },
                        expected_runtime_incarnation=runtime,
                    ):
                        return False

                    extracted = await self._extract_snapshot_to_k8s_pod(
                        job_id,
                        pod_ip,
                        attestation.port,
                        expected_runtime_incarnation=runtime,
                        expected_host_key_fingerprint=(host_fingerprint),
                        mutation_authority=mutation_authority,
                    )
                    if extracted:
                        repaired = await self._repair_git_after_snapshot(
                            job_id,
                            job,
                            pod_ip,
                            attestation.port,
                            backend="sandbox",
                            expected_host_key_fingerprint=(host_fingerprint),
                            mutation_authority=mutation_authority,
                        )
                        if job.get("repo_name") and not repaired:
                            logger.warning(
                                "Scoped Git repair failed for IDE session job %s",
                                job_id,
                            )
                        else:
                            await self._seed_ide_profile_for_user(
                                job_id,
                                job,
                                pod_ip,
                                attestation.port,
                                expected_host_key_fingerprint=(host_fingerprint),
                                mutation_authority=mutation_authority,
                            )

                            # code-server should already be running from the
                            # workspace entrypoint. Keep its bounded health
                            # probe under the renewable exact-B lease.
                            ready = await self._wait_for_code_server(
                                f"http://{pod_ip}:38080", timeout=15
                            )
                            if not ready:
                                logger.warning(
                                    "code-server not responding on IDE pod %s — "
                                    "setting active anyway",
                                    pod_name,
                                )
                            post_attestation = await mutation_authority()
                            effects_ok = post_attestation == attestation and (
                                await self._container_provisioner.ide_pod_live(
                                    job_id,
                                    expected_runtime_incarnation=runtime,
                                )
                                is True
                            )
            except (RestoreWorkLeaseLost, WorkspaceRuntimeAuthorityError):
                logger.warning("IDE restore-work authority changed for job %s", job_id)
                return False

            if not settle_restore_work:
                return effects_ok
            if not effects_ok:
                return False
            if not await self._complete_ide_restore_work(
                job_id,
                restore_work=restore_work,
                claimant=claimant,
                success=True,
            ):
                return False
            restore_work_finished = True

            logger.info(
                "IDE session active (K8s container snapshot restore) for job %s: %s",
                job_id,
                _build_code_server_url(job_id),
            )
            return True
        finally:
            if owns_claim and not restore_work_finished:
                await self._release_ide_restore_work(
                    job_id,
                    restore_work=restore_work,
                    claimant=claimant,
                )

    async def _extract_snapshot_to_k8s_pod(
        self,
        job_id: str,
        pod_ip: str,
        ssh_port: int = 30022,
        *,
        expected_runtime_incarnation: str | None = None,
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> bool:
        """Download S3 snapshot and extract into the IDE pod via SSH."""
        import tempfile

        async def fail(error: str) -> None:
            updates = {"status": "failed", "error": error}
            if expected_runtime_incarnation is None:
                await self._set_session_context(job_id, updates)
            else:
                await self._set_session_context_if_runtime(
                    job_id,
                    updates,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )

        if not self._snapshot_service:
            await fail("Snapshot service not available")
            return False

        if not getattr(self._snapshot_service, "is_available", True):
            await fail("Snapshot service is unavailable")
            return False

        with tempfile.NamedTemporaryFile(
            suffix=".tar.zst", delete=True, prefix=f"restore_{job_id[:8]}_"
        ) as tmp:
            tar_path = tmp.name

            ok = await self._snapshot_service.download_snapshot(job_id, tar_path)
            if not ok:
                error_msg = "Failed to download snapshot"
                logger.warning(
                    "Failed to download snapshot for job %s",
                    job_id,
                )
                await fail(error_msg)
                return False

            # Snapshot download is local. Re-attest and re-lock immediately
            # before the first archive byte can leave for a mutable Pod IP.
            if mutation_authority is not None:
                attestation = await mutation_authority()
                pod_ip = attestation.pod_ip
                ssh_port = attestation.port
                expected_host_key_fingerprint = attestation.ssh_host_key_fingerprint

            key_path = resolve_ssh_key_path()
            if not key_path:
                logger.warning(
                    "No SSH key available for snapshot extraction for job %s",
                    job_id,
                )
            rc, stderr = await stream_extract_snapshot(
                pod_ip,
                ssh_port,
                tar_path,
                key_path=key_path,
                remote_cmd=EXTRACT_HOME_REMOTE_CMD,
                expected_host_key_fingerprint=expected_host_key_fingerprint,
            )
            if rc != 0:
                # tar exits 2 on any per-file error even when the payload
                # extracted fine (e.g. an unwritable stray path). Probe the
                # workspace before declaring failure — a populated workspace
                # beats a hard error for a browse tool.
                if await self._workspace_populated(
                    pod_ip,
                    ssh_port,
                    expected_host_key_fingerprint=(expected_host_key_fingerprint),
                ):
                    logger.warning(
                        "Snapshot extraction for job %s exited rc=%d but the "
                        "workspace is populated — continuing: %s",
                        job_id,
                        rc,
                        stderr.decode(errors="replace")[:300],
                    )
                    return True
                error_msg = (
                    f"Snapshot extraction failed for job {job_id} (rc={rc}): "
                    f"{stderr.decode(errors='replace')[:500]}"
                )
                logger.warning(error_msg)
                await fail(error_msg)
                return False

        return True

    async def _workspace_populated(
        self,
        ssh_host: str,
        ssh_port: int,
        *,
        expected_host_key_fingerprint: str | None = None,
    ) -> bool:
        """True when the pod's workspace directory exists and is non-empty."""
        import asyncio

        probe_cmd = 'test -n "$(ls -A /home/agent-host/workspace 2>/dev/null)"'
        try:
            if expected_host_key_fingerprint is None:
                return False
            async with pinned_agent_ssh_command(
                ssh_host,
                ssh_port,
                probe_cmd,
                expected_host_key_fingerprint=expected_host_key_fingerprint,
            ) as command:
                proc = await create_owned_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                return (await wait_bounded(proc, timeout=20)) == 0
        except Exception:
            return False

    async def _repair_git_after_snapshot(
        self,
        job_id: str,
        job: dict,
        ssh_host: str,
        ssh_port: int,
        *,
        backend: str = "sandbox",
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> bool:
        """Replace legacy origin authority and refetch after snapshot restore.

        Snapshot capture excludes ``.git/objects`` (snapshot_service's
        exclude list — "re-cloned/regenerated on restore"), so the restored
        workspace repo has config/refs/index but no objects. A fetch
        repopulates them. This step is now an authorization boundary: a legacy
        snapshot can contain an administrator-bearing origin, so a repo-backed
        IDE may not become reachable unless that origin has been replaced by
        and proven through the scoped deploy-key transport.
        """
        repo_name = job.get("repo_name")
        if not repo_name:
            return True
        branch = job.get("branch_name") or "main"
        repaired = await self._install_and_sync_managed_repository_over_ssh(
            job_id,
            backend=backend,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            branch=branch,
            workspace_path="/home/agent-host/workspace",
            require_existing=True,
            expected_host_key_fingerprint=expected_host_key_fingerprint,
            mutation_authority=mutation_authority,
        )
        if not repaired:
            logger.info("Scoped Git repair failed for IDE session job %s", job_id)
        return repaired

    async def _seed_ide_profile_for_user(
        self,
        job_id: str,
        job: dict,
        ssh_host: str,
        ssh_port: int,
        *,
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> None:
        """Seed IDE config and profile for restored IDE pods."""
        try:
            from orchestrator.services.ide_settings import seed_ide_config_for_user

            await seed_ide_config_for_user(
                self._db,
                job.get("user_id"),
                ssh_host,
                ssh_port,
                expected_host_key_fingerprint=(expected_host_key_fingerprint),
                mutation_authority=(
                    (lambda: self._ide_mutation_target_tuple(mutation_authority))
                    if mutation_authority is not None
                    else None
                ),
            )

            # Restore license/globalStorage + non-Open-VSX bytes into the IDE
            # workspace. Best-effort; no-ops when optional dependencies
            # are unavailable.
            user_id = job.get("user_id")
            if (
                user_id
                and self._snapshot_service
                and getattr(self._snapshot_service, "is_available", True)
            ):
                from orchestrator.services.ide_profile_store import IdeProfileStore
                from orchestrator.services.ide_settings import (
                    IdeSettingsStore,
                    seed_ide_profile,
                )

                settings_store = IdeSettingsStore(self._db)
                items = await settings_store.get_extensions(str(user_id))
                profile_pointers = await settings_store.get_profile_pointers(
                    str(user_id)
                )
                profile = IdeProfileStore(
                    self._snapshot_service._s3, self._snapshot_service._bucket
                )
                await seed_ide_profile(
                    user_id=str(user_id),
                    ssh_host=ssh_host,
                    ssh_port=ssh_port,
                    profile_store=profile,
                    ext_items=items,
                    profile_pointers=profile_pointers,
                    expected_host_key_fingerprint=(expected_host_key_fingerprint),
                    mutation_authority=(
                        (lambda: self._ide_mutation_target_tuple(mutation_authority))
                        if mutation_authority is not None
                        else None
                    ),
                )
        except Exception:
            logger.warning(
                "Failed to seed code-server config for IDE session %s",
                job_id,
                exc_info=True,
            )

    @staticmethod
    async def _ide_mutation_target_tuple(
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]],
    ) -> tuple[str, int, str]:
        attestation = await mutation_authority()
        return (
            attestation.pod_ip,
            attestation.port,
            attestation.ssh_host_key_fingerprint,
        )

    async def _wait_for_vm_ready(
        self, job_id: str, timeout: int = _VM_READY_TIMEOUT_SECONDS
    ) -> tuple[Optional[str], Optional[int]]:
        """Poll job context until VM is ready, return (ssh_host, ssh_port)."""
        import asyncio

        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        while datetime.now(timezone.utc) < deadline:
            job = await self._get_job(job_id)
            if not job:
                return None, None

            ctx = self._parse_context(job)
            vm_ctx = ctx.get("vm", {})
            ssh_host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
            if ssh_host and not orchestrator_can_reach(ssh_host):
                return None, None

            if vm_ctx.get("status") == "ready":
                ssh_port = vm_ctx.get("ssh_port") or 22
                if ssh_host:
                    return ssh_host, int(ssh_port)

            if vm_ctx.get("status") == "failed":
                return None, None

            await asyncio.sleep(3)

        return None, None

    async def _extract_snapshot_to_vm(
        self,
        job_id: str,
        ssh_host: str,
        ssh_port: int,
        *,
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> bool:
        """Download snapshot from S3 and extract into the VM via SSH.

        Mirrors workspace_suspension.py:_extract_snapshot (lines 506-562).

        Returns:
            True when the snapshot was downloaded AND unpacked cleanly; False
            when there is no snapshot service, the download failed, or tar
            exited non-zero. Callers MUST NOT report the workspace restored
            on False — this used to return None on every path, so a failed
            restore still reported success and the agent resumed on an empty
            or half-populated tree.
        """
        import tempfile

        if not self._snapshot_service:
            return False
        if expected_host_key_fingerprint is None or mutation_authority is None:
            logger.warning(
                "VM snapshot extraction refused without exact runtime authority "
                "for job %s",
                job_id,
            )
            return False
        try:
            initial = await mutation_authority()
        except Exception:
            return False
        if (
            initial.host != ssh_host
            or initial.port != ssh_port
            or initial.ssh_host_key_fingerprint != expected_host_key_fingerprint
        ):
            return False

        with tempfile.NamedTemporaryFile(
            suffix=".tar.zst", delete=True, prefix=f"restore_{job_id[:8]}_"
        ) as tmp:
            tar_path = tmp.name

            # Download from S3
            ok = await self._snapshot_service.download_snapshot(job_id, tar_path)
            if not ok:
                logger.warning("Failed to download snapshot for job %s", job_id)
                return False

            # Extract into VM via SSH
            key_path = resolve_ssh_key_path()
            if not key_path:
                logger.warning(
                    "No SSH key available for snapshot extraction (job %s)",
                    job_id,
                )
            try:
                current = await mutation_authority()
            except Exception:
                return False
            if current != initial:
                return False
            rc, stderr = await stream_extract_snapshot(
                ssh_host,
                ssh_port,
                tar_path,
                key_path=key_path,
                expected_host_key_fingerprint=expected_host_key_fingerprint,
            )

            if rc != 0:
                logger.warning(
                    "Snapshot extraction had errors for job %s (rc=%d): %s",
                    job_id,
                    rc,
                    stderr.decode(errors="replace")[:500],
                )
                return False

            try:
                return await mutation_authority() == initial
            except Exception:
                return False

    async def _clone_gitea_to_vm(
        self,
        job_id: str,
        job: dict,
        ssh_host: str,
        ssh_port: int,
        *,
        expected_host_key_fingerprint: str | None = None,
        mutation_authority: Callable[[], Awaitable[WorkspaceRuntimeAttestation]]
        | None = None,
    ) -> bool:
        """Clone the job's Gitea repo into the VM as a fallback."""
        repo_name = job.get("repo_name")
        if (
            not repo_name
            or expected_host_key_fingerprint is None
            or mutation_authority is None
        ):
            return False
        branch = job.get("branch_name") or "main"
        return await self._install_and_sync_managed_repository_over_ssh(
            job_id,
            backend="vm",
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            branch=branch,
            workspace_path="/home/agent-host/workspace",
            expected_host_key_fingerprint=expected_host_key_fingerprint,
            mutation_authority=mutation_authority,
        )

    async def _delete_ide_vm(self, job_id: str, vm_name: str) -> bool:
        """Delete an IDE session VM."""
        if self._vm_provisioner and self._vm_provisioner.lifecycle_available:
            try:
                identity = await self._vm_provisioner.capture_vm_teardown_identity(
                    job_id, entity_type="job"
                )
                permit = await acquire_vm_cleanup_permit(
                    self._workspace_recovery_store,
                    owner_kind="job",
                    owner_id=job_id,
                    identity=identity,
                    source="ide_session_vm_delete",
                    purge_disk=True,
                )
                if not permit.allowed:
                    return False
                disposition = completed_cleanup_outcome(permit)
                if disposition is None:
                    outcome = await self._vm_provisioner.release_vm_captured(
                        job_id,
                        identity,
                        entity_type="job",
                        purge_disk=True,
                        capture_snapshot=False,
                        **vm_cleanup_kwargs(permit),
                    )
                    disposition = outcome.disposition
                    if disposition in {"completed", "identity_superseded"}:
                        await complete_vm_cleanup_permit(
                            self._workspace_recovery_store,
                            permit,
                            outcome=disposition,
                            provisioner=self._vm_provisioner,
                        )
                elif disposition == "completed":
                    await complete_vm_cleanup_permit(
                        self._workspace_recovery_store, permit,
                        outcome=disposition,
                        provisioner=self._vm_provisioner,
                    )
                return disposition == "completed"
            except Exception:
                logger.exception("IDE VM cleanup authority failed for job %s", job_id)
                return False
        return False

    async def _delete_ide_container(
        self,
        job_id: str,
        container_name: str,
        restore_type: str = "container",
        *,
        expected_container_id: str | None = None,
        expected_runtime_incarnation: str | None = None,
    ) -> bool:
        """Stop and remove an IDE session container (K8s pod or local container)."""
        if restore_type == "k8s_container":
            if self._container_provisioner:
                return bool(
                    await self._container_provisioner.delete_ide_pod(
                        job_id,
                        expected_runtime_incarnation=expected_runtime_incarnation,
                    )
                )
            return False

        # Local dev path (podman/docker)
        try:
            runtime = await self._detect_container_runtime()
            if (
                expected_container_id is None
                or re.fullmatch(r"[0-9a-f]{64}", expected_container_id) is None
            ):
                return False
            observed = await self._inspect_container_id(runtime, container_name)
            if observed is not None and observed != expected_container_id:
                return False
            if self._db is None:
                return False
            already_retired = bool(
                await self._db.managed_repository_workspace_process_zero_is_current(
                    job_id,
                    owner_kind="job",
                    scope="ide_local",
                    provisioner="docker",
                    runtime_incarnation=expected_container_id,
                )
            )
            if observed is None:
                if not already_retired:
                    return False
                return await self._remove_container(
                    runtime,
                    container_name,
                    expected_container_id=expected_container_id,
                )
            if (
                not already_retired
                and not await self._db.claim_managed_repository_workspace_retirement(
                    job_id,
                    owner_kind="job",
                    scope="ide_local",
                    provisioner="docker",
                    runtime_incarnation=expected_container_id,
                )
            ):
                return False
            retire = (
                managed_repository_agent_retirement_command(
                    home_path="/home/coder",
                    authority_ids=None,
                    remove_configs=True,
                )
                + "; "
                + managed_repository_agent_zero_command(home_path="/home/coder")
            )
            process = await create_owned_subprocess_exec(
                runtime,
                "exec",
                expected_container_id,
                "sh",
                "-c",
                retire,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            if await wait_bounded(process, timeout=120) != 0:
                return False
            observed_after = await self._inspect_container_id(runtime, container_name)
            if observed_after is not None and observed_after != expected_container_id:
                return False
            if not await self._db.record_managed_repository_workspace_process_zero(
                job_id,
                owner_kind="job",
                scope="ide_local",
                provisioner="docker",
                runtime_incarnation=expected_container_id,
            ):
                return False
            return await self._remove_container(
                runtime,
                container_name,
                expected_container_id=expected_container_id,
            )
        except Exception as e:
            logger.warning("Failed to remove IDE container %s: %s", container_name, e)
            return False

    async def _delete_k8s_ide_container_with_outcome(
        self,
        job_id: str,
        *,
        expected_runtime_incarnation: str | None,
    ) -> Any:
        """Return the provisioner's explicit immutable-runtime outcome."""

        if self._container_provisioner is None:
            from orchestrator.services.container_provisioner import (
                RuntimeDeletionOutcome,
            )

            return RuntimeDeletionOutcome("refused")
        return await self._container_provisioner.delete_ide_pod_with_outcome(
            job_id,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )

    # =========================================================================
    # Snapshot restore for job resume
    # =========================================================================

    async def restore_snapshot_for_resume(
        self, job_id: str, ssh_host: str, ssh_port: int
    ) -> bool:
        """Refuse the legacy pre-claim VM resume snapshot path."""
        if not self._snapshot_service or not self._snapshot_service.is_available:
            return False

        # Public resume has not yet acquired the job/agent/execution lease when
        # this legacy hook runs, so it cannot own a durable VM restore
        # operation.  Refuse before S3 download or SSH; the authoritative
        # dispatch/resume path must perform restoration under a future exact
        # generation receipt instead of trusting caller-provided coordinates.
        logger.warning(
            "VM resume snapshot extraction refused without durable resume "
            "authority for job %s",
            job_id,
        )
        return False

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _get_job(self, job_id: str) -> Optional[dict]:
        """Fetch job from DB."""
        if not self._db:
            return None
        return await self._db.get_job(job_id)

    @staticmethod
    def _parse_context(job: dict) -> dict:
        """Parse job context, handling both dict and JSON string."""
        ctx = job.get("context") or {}
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except json.JSONDecodeError:
                ctx = {}
        return ctx

    async def _set_session_context(self, job_id: str, updates: dict) -> None:
        """Atomically merge updates into context.ide_session."""
        if not self._db:
            return
        try:
            await self._db.merge_ide_session_context(job_id, updates)
        except Exception:
            logger.exception("Failed to update IDE session context for job %s", job_id)

    async def _set_session_context_if_runtime(
        self,
        job_id: str,
        updates: dict,
        *,
        expected_runtime_incarnation: str | None,
    ) -> bool:
        """Merge lifecycle state only while the captured Pod UID is current.

        The class-level lookup is intentional: permissive mocks must not
        fabricate a production authority seam dynamically. Missing or invalid
        immutable runtime identity fails closed.
        """

        if self._db is None or not isinstance(expected_runtime_incarnation, str):
            return False
        try:
            if str(UUID(expected_runtime_incarnation)) != expected_runtime_incarnation:
                return False
        except (TypeError, ValueError):
            return False

        merge_if_current = getattr(
            type(self._db),
            "merge_ide_session_context_if_runtime",
            None,
        )
        if not callable(merge_if_current):
            return False
        try:
            return bool(
                await merge_if_current(
                    self._db,
                    job_id,
                    updates,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
            )
        except Exception:
            logger.exception(
                "Failed to update exact IDE runtime context for job %s",
                job_id,
            )
            return False

    @staticmethod
    def _compute_expiry(session_ctx: dict) -> Optional[str]:
        """Compute when the session will expire."""
        started_at = session_ctx.get("started_at")
        max_lifetime = session_ctx.get("max_lifetime_minutes", 240)
        if started_at:
            start = datetime.fromisoformat(started_at)
            expiry = start + timedelta(minutes=max_lifetime)
            return expiry.isoformat()
        return None

    # =========================================================================
    # Container helpers (Gitea fallback)
    # =========================================================================

    @staticmethod
    async def _detect_container_runtime() -> str:
        """Detect available container runtime (podman or docker)."""
        import shutil

        for runtime in ("podman", "docker"):
            if shutil.which(runtime):
                return runtime

        raise RuntimeError("No container runtime found (podman or docker)")

    @staticmethod
    async def _find_free_port() -> int:
        """Find a free TCP port on localhost."""
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    @staticmethod
    async def _wait_for_code_server(url: str, timeout: int = 30) -> bool:
        """Poll code-server URL until it responds."""
        import asyncio
        import httpx

        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        async with httpx.AsyncClient(timeout=5.0) as client:
            while datetime.now(timezone.utc) < deadline:
                try:
                    resp = await client.get(url)
                    if resp.status_code < 500:
                        return True
                except (
                    httpx.ConnectError,
                    httpx.ReadTimeout,
                    httpx.RemoteProtocolError,
                ):
                    pass
                await asyncio.sleep(1)
        return False

    @staticmethod
    async def _inspect_container_id(
        runtime: str,
        container_name: str,
    ) -> str | None:
        """Resolve a mutable local-container name only as an ID assertion."""

        process = await create_owned_subprocess_exec(
            runtime,
            "inspect",
            "--format",
            "{{.Id}}",
            container_name,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await communicate_bounded(
            process,
            timeout=30,
            stdout_limit=4096,
            stderr_limit=64 * 1024,
        )
        if process.returncode != 0:
            return None
        observed = stdout.decode(errors="replace").strip()
        return observed if re.fullmatch(r"[0-9a-f]{64}", observed) else None

    @staticmethod
    async def _remove_container(
        runtime: str,
        container_name: str,
        *,
        expected_container_id: str | None = None,
    ) -> bool:
        """Remove only the exact captured local IDE container identity."""
        import asyncio

        if (
            expected_container_id is None
            or re.fullmatch(r"[0-9a-f]{64}", expected_container_id) is None
        ):
            return False
        # Resolve the mutable name only as a consistency assertion. Destruction
        # below targets the immutable full ID so a same-name replacement is
        # never inherited after a lost response.
        observed = await IdeSessionService._inspect_container_id(
            runtime, container_name
        )
        if observed is not None and observed != expected_container_id:
            return False

        proc = await create_owned_subprocess_exec(
            runtime,
            "rm",
            "-f",
            expected_container_id,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await wait_bounded(proc, timeout=60)
        probe = await create_owned_subprocess_exec(
            runtime,
            "ps",
            "-a",
            "--no-trunc",
            "--format",
            "{{.ID}}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await communicate_bounded(
            probe,
            timeout=30,
            stdout_limit=1024 * 1024,
            stderr_limit=64 * 1024,
        )
        if probe.returncode != 0:
            return False
        return expected_container_id not in stdout.decode(errors="replace").splitlines()


# Module-level singleton
ide_session_service = IdeSessionService()
