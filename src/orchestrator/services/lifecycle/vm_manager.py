"""VMInstanceManager — Phase 3 stateful manager for VM workspaces.

Wraps ``VMProvisioner`` (4-backend internal dispatch:
NATS / HTTP / direct KubeVirt / Docker pool), the snapshot service,
and the suspension service. Surfaces VMs to the unified lifecycle
reconciler so drift detection, crash recovery, and snapshot-before-
delete work the same way they do for agents and workspaces.

Unlike pods, VMs aren't enumerated via a K8s label selector — each
backend has its own listing convention, and the orchestrator already
keys VMs by job/thread id via ``jobs.context.vm`` and
``threads.metadata.vm`` JSONB. The manager iterates those rows
instead. The 4-backend dispatch stays inside ``VMProvisioner``; from
the lifecycle reconciler's perspective there is one VM kind.

Phase 3 scope: drift detection + crash recovery via the existing
``vm.status`` field. Deeper health probes (NATS daemon ping, HTTP
controller health endpoint) are a follow-up — they require new
provisioner methods, not new lifecycle scaffolding.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator
from uuid import NAMESPACE_URL, UUID, uuid5

from orchestrator.services.completion_lifecycle import (
    CompletionLifecycleOwnership,
    LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS,
    LifecycleActionPermit,
    LifecycleRouteDecision,
)
from orchestrator.services.blocking_effect import joined_blocking_call
from orchestrator.services.ssh_helpers import orchestrator_can_reach
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    bind_vm_cleanup_permit,
    vm_cleanup_kwargs,
    cleanup_intent_digest,
    completed_cleanup_outcome,
)

from orchestrator.services.lifecycle.types import Instance
from orchestrator.services.lifecycle.workspace_manager import (
    infra_transient_retry_pending,
    orphan_grace_seconds,
    paused_within_grace,
)

logger = logging.getLogger(__name__)


# 'reviewing' is the verification-enabled twin of 'pending_review'. By *status*
# it is as quiescent as 'pending_review', but a critic subjob SSHes into the
# *parent's* live VM (inherited context.vm), so reaping on status alone kills the
# VM under the critic. The is_idle/is_reapable predicates therefore gate on
# ``has_live_shared_child`` (a non-terminal child bound to this same VM); the
# status set stays optimistic and the guard handles the dependency precisely.
# Kept in lockstep with WorkspaceInstanceManager._IDLE_JOB_STATUSES. (In the
# normal flow the guard, not the status set, is what keeps a reviewing VM alive:
# a live critic pins it, and by the time the critic ends the parent has moved to
# pending_review/completed — so the guard also avoids churning the VM's
# expensive ~3-min disk re-clone, see srw_vm_dispatcher_reconciler_churn.)
# See knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
_IDLE_JOB_STATUSES = frozenset(
    {"paused", "pending_review", "reviewing", "waiting_for_reply"}
)
_IDLE_THREAD_STATUSES = frozenset({"ended"})

# Terminal = bound work finished; reapable = pod/VM no longer needed at all
# (suspendable-idle ∪ terminal). Mirrors WorkspaceInstanceManager.
_TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TERMINAL_THREAD_STATUSES = frozenset({"ended"})
_REAPABLE_JOB_STATUSES = _IDLE_JOB_STATUSES | _TERMINAL_JOB_STATUSES
_REAPABLE_THREAD_STATUSES = _IDLE_THREAD_STATUSES | _TERMINAL_THREAD_STATUSES

# VM teardown is fire-and-forget to an external controller, and the DB
# context.vm row persists (status flips to 'deleting', later 'deleted', or
# 'suspended' on a clean suspend) until a fresh provision clears it. Unlike
# workspace pods — which vanish from the live pod list once deleted — a
# torn-down VM would otherwise stay enumerable and get re-reaped every tick.
# Skip these statuses so reap/drift/crash don't loop on a VM that's already
# gone or restorable-on-demand. ('deleted'/'none'/'suspended' are handled by
# ensure_workspace on the next dispatch: recreate or restore.)
_TORN_DOWN_VM_STATUSES = frozenset({"deleting", "deleted", "suspended"})

# 'failed' is exclusively a *provisioning* failure (the only three writers of
# vm_status='failed' are create_vm exception paths — there is no runtime-failed
# source). The dispatcher deliberately PARKS a 'failed' VM ("do NOT hot-retry
# every tick (shared VM cluster)"), so the reconciler must stay off it: reaping
# it (is_healthy=False → force-delete → status 'deleted') would flip it back to
# the dispatcher's re-provision branch and churn. Skip like torn-down.
_PARKED_VM_STATUSES = frozenset({"failed"})


def expected_vm_shas() -> set[str]:
    """SHAs from the configured default VM image tag.

    Reads ``DEFAULT_VM_IMAGE`` and extracts the suffix from a
    ``...:sha-<hash>`` tag. Per-job ``vm_image`` overrides aren't
    surfaced here — drift is measured against the orchestrator's
    current default. Returns an empty set for ``:latest``-style tags
    so drift checks short-circuit cleanly in local dev.
    """
    shas: set[str] = set()
    tag = os.environ.get("DEFAULT_VM_IMAGE", "")
    if ":sha-" in tag:
        shas.add(tag.rsplit(":sha-", 1)[-1])
    return shas


def _extract_sha(image: str | None) -> str | None:
    if not image or ":sha-" not in image:
        return None
    return image.rsplit(":sha-", 1)[-1]


def _vm_age_seconds(created_at: Any) -> float | None:
    """Age from a KubeVirt creationTimestamp (ISO string over the wire)."""
    if isinstance(created_at, str):
        try:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return None
    elif isinstance(created_at, datetime):
        created = created_at
    else:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - created).total_seconds())


def _coerce_jsonb(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


def _is_stateless_instance(inst: Instance) -> bool:
    """Whether a VM is owned by an exact stateless job or thread row.

    Stateless physical-instance retirement is acknowledged by its terminal/loss
    protocol.  The older generic lifecycle manager has no such acknowledgement,
    so it must stay observational for these rows.  Exact matching deliberately
    leaves pinned, missing, malformed, and future lane values on their existing
    path.
    """
    return (
        inst.metadata.get("scope") in ("job", "thread")
        and inst.metadata.get("execution_lane") == "stateless"
    )


class VMInstanceManager:
    """Lifecycle manager for the vm kind."""

    kind = "vm"

    def __init__(
        self,
        vm_provisioner: Any,
        suspension_service: Any,
        snapshot_service: Any,
        db: Any,
        *,
        completion_commands_enabled: bool = False,
        completion_router: Any | None = None,
        workspace_recovery_store: Any | None = None,
    ):
        self._provisioner = vm_provisioner
        self._suspension = suspension_service
        self._snapshot = snapshot_service
        self._db = db
        # Constructor gating keeps the rollback path byte-for-byte legacy: with
        # completion commands disabled, no lifecycle query names or reads the
        # reserved control marker.
        self._completion_commands_enabled = completion_commands_enabled
        self._completion_lifecycle = (
            CompletionLifecycleOwnership(db, completion_router)
            if completion_commands_enabled and completion_router is not None
            else None
        )
        self._workspace_recovery_store = (
            workspace_recovery_store or VMWorkspaceRecoveryStore(db)
        )
        # Reachability probe cache: ssh_host -> (probed_at, ok).
        self._reach_cache: dict[str, tuple[float, bool]] = {}
        self._reach_ttl_s: float = 30.0
        self._clock = time.monotonic

    @property
    def completion_lifecycle_ownership_enabled(self) -> bool:
        return self._completion_lifecycle is not None

    # -------------------------------------------------------------------------
    # Protocol implementation
    # -------------------------------------------------------------------------

    async def expected_versions(self) -> set[str]:
        return expected_vm_shas()

    async def list_instances(self) -> list[Instance]:
        if not self._provisioner_available():
            return []
        rows = await self._fetch_vm_rows()
        instances: list[Instance] = []
        for row in rows:
            inst = self._row_to_instance(row)
            if inst is None:
                continue
            if (
                self._completion_lifecycle is not None
                and inst.metadata.get("scope") == "job"
                and inst.bound_to
            ):
                decision = await self._completion_lifecycle.classify(
                    inst.bound_to, source="lifecycle_vm_list"
                )
                inst.metadata["completion_lifecycle_disposition"] = decision.disposition
                inst.metadata["completion_lifecycle_route"] = decision.route
                inst.metadata["completion_lifecycle_command_id"] = decision.command_id
                inst.metadata["completion_lifecycle_deferred"] = decision.deferred
                inst.metadata["completion_control_owned"] = (
                    decision.reason == "active_control_claim"
                )
            # Live-child guard: only a reapable-status *job* VM can be torn down,
            # and only a job VM can have a critic subjob, so scope the query to
            # that case. Threads have no critic children.
            native_id = inst.metadata.get("vm_native_id")
            if (
                inst.metadata.get("scope") == "job"
                and inst.bound_to
                and native_id
                and inst.metadata.get("job_status") in _REAPABLE_JOB_STATUSES
            ):
                inst.metadata[
                    "has_live_shared_child"
                ] = await self._live_shared_child_exists(inst.bound_to, native_id)
            instances.append(inst)
        return instances

    async def is_healthy(self, inst: Instance) -> bool:
        if _is_stateless_instance(inst):
            # Suppress the reconciler's immediate unhealthy-delete shortcut;
            # terminal/loss retirement is the sole physical cleanup owner.
            return True
        if inst.metadata.get("completion_lifecycle_deferred"):
            return True
        # VMs report status in their own context. ``failed`` (a provisioning
        # failure) is now filtered out in _row_to_instance (_PARKED_VM_STATUSES)
        # so the dispatcher can hold it parked without the reconciler force-
        # deleting it — this branch is kept as defense-in-depth in case a failed
        # VM ever reaches here. ``suspended`` means already drained. Anything
        # else (provisioning/created/ready/restoring) is treated as healthy.
        status = inst.metadata.get("vm_status")
        if status == "failed":
            return False
        return True

    async def is_idle(self, inst: Instance) -> bool:
        if _is_stateless_instance(inst):
            return False
        if inst.metadata.get("completion_lifecycle_deferred"):
            return False
        if inst.metadata.get("has_live_shared_child"):
            return False
        ide_session_status = inst.metadata.get("ide_session_status")
        if ide_session_status in ("restoring", "active", "idle"):
            return False
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        if job_status:
            # Warm grace for 'paused' (often a human-wait — sudo/VM-upgrade
            # approval with a 24h TTL); VM reaps are destructive (no volume
            # reattach), so acting on the next tick loses the workspace
            # minutes into that window. Mirrors WorkspaceInstanceManager.
            if paused_within_grace(inst.metadata):
                return False
            # A job paused for a transient-infra retry is NOT idle — it is
            # mid-recovery and will resume onto this exact VM. Reaping it would
            # hand the retry an empty workspace, which is the loss the retry
            # exists to avoid. Bounded by next_retry_at (≤1h). (Defect 1b)
            if infra_transient_retry_pending(inst.metadata):
                return False
            return job_status in _IDLE_JOB_STATUSES
        if thread_status:
            return thread_status in _IDLE_THREAD_STATUSES
        return False

    # -------------------------------------------------------------------------
    # Reap path (ReapableInstanceManager) — mirrors WorkspaceInstanceManager,
    # adjusted for VMs (ssh port default 22; vm_status='suspended' == clean;
    # give_up is force-delete — no volume reattach this round).
    # -------------------------------------------------------------------------

    async def is_reapable(self, inst: Instance) -> bool:
        """True when the bound work no longer needs the VM (idle ∪ terminal).

        A *dispatchable* job (created/paused, unassigned, freeze-free) is not
        idle — it is MID-RESUME, owned by the dispatcher, which is actively
        (re)provisioning or about to claim its VM. Reaping such a VM fought the
        dispatcher head-on: a version_upgrade drain leaves the VM 'ready', the
        dispatcher re-claims on resume, but the reconciler saw 'paused' and tore
        it down first — create→reap→create churn against the shared VM cluster,
        and a needless ~3-min disk re-clone on every resume. Hand off: the
        dispatcher owns bring-up; the reaper only reclaims genuinely idle VMs
        (pending_review / waiting_for_reply / a paused-but-still-frozen job) and
        terminal ones. Mirrors get_dispatchable_jobs' predicate exactly.

        Guard: a VM shared by a live child job (a critic SSHed into the parent's
        VM) is never reapable, regardless of the parent's own status. See
        ``_live_shared_child_exists`` and
        knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
        """
        if _is_stateless_instance(inst):
            return False
        if inst.metadata.get("completion_lifecycle_deferred"):
            return False
        if inst.metadata.get("has_live_shared_child"):
            return False
        ide_session_status = inst.metadata.get("ide_session_status")
        if ide_session_status in ("restoring", "active", "idle"):
            return False
        if inst.metadata.get("job_dispatchable"):
            return False
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        if job_status:
            # Warm grace for 'paused' — see is_idle.
            if paused_within_grace(inst.metadata):
                return False
            # A job paused for a transient-infra retry is NOT idle — it is
            # mid-recovery and will resume onto this exact VM. Reaping it would
            # hand the retry an empty workspace, which is the loss the retry
            # exists to avoid. Bounded by next_retry_at (≤1h). (Defect 1b)
            if infra_transient_retry_pending(inst.metadata):
                return False
            return job_status in _REAPABLE_JOB_STATUSES
        if thread_status:
            return thread_status in _REAPABLE_THREAD_STATUSES
        return False

    def _is_terminal(self, inst: Instance) -> bool:
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        if job_status:
            return job_status in _TERMINAL_JOB_STATUSES
        if thread_status:
            return thread_status in _TERMINAL_THREAD_STATUSES
        return False

    async def is_dirty(self, inst: Instance) -> bool:
        """True when the VM may hold un-snapshotted state worth saving.

        Same logic as the workspace manager (threads use ``total_turns`` vs
        ``last_snapshot_turns``; jobs are conservative), plus a VM wrinkle:
        ``vm_status == 'suspended'`` means the VM was already snapshotted to S3
        and torn down, so there is nothing left to lose — treat as clean.
        Never reads ``last_activity`` (contaminated by context merges).
        """
        if inst.metadata.get("vm_status") == "suspended":
            return False
        thread_status = inst.metadata.get("thread_status")
        if thread_status is not None:
            turns = inst.metadata.get("total_turns") or 0
            snap_turns = inst.metadata.get("last_snapshot_turns")
            if snap_turns is None:
                return turns > 0
            return turns > snap_turns
        if self._is_terminal(inst):
            return inst.metadata.get("snapshot_status") != "available"
        return True

    async def _tcp_probe(self, host: str, port: int) -> bool:
        """One-shot TCP connect with a short timeout. Overridable in tests."""

        def _connect() -> bool:
            try:
                with socket.create_connection((host, port), timeout=5):
                    return True
            except OSError:
                return False

        # Cancellation of ``to_thread`` alone abandons the live socket worker.
        # Join the bounded (five-second) probe before lifecycle ownership can
        # move on to a replacement or retirement attempt.
        return await joined_blocking_call(_connect)

    async def is_reachable(self, inst: Instance) -> bool:
        """Cached liveness probe to the VM's SSH endpoint.

        VMs run sshd on 22 by default (not 30022 like workspace pods). Used
        only in the reap path to choose snapshot-vs-retry; cached ~30s per host.
        """
        host = inst.metadata.get("ssh_host")
        if not host:
            return False
        port = int(inst.metadata.get("ssh_port") or 22)
        now = self._clock()
        cached = self._reach_cache.get(host)
        if cached is not None and (now - cached[0]) < self._reach_ttl_s:
            return cached[1]
        ok = await self._tcp_probe(host, port)
        self._reach_cache[host] = (now, ok)
        return ok

    def _max_attempts(self) -> int:
        try:
            return int(os.environ.get("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5"))
        except ValueError:
            return 5

    async def attempts_exhausted(self, inst: Instance) -> bool:
        # A tailnet-addressed VM the orchestrator has no route to can NEVER be
        # snapshotted from this vantage — every attempt is a guaranteed miss,
        # and the bounded retry just delays the force-delete by
        # max_attempts × tick (~5 min observed) while the dead VM holds cluster
        # capacity. Skip straight to exhausted. Honors the
        # ORCHESTRATOR_HAS_TAILNET_ROUTE escape hatch via orchestrator_can_reach
        # (see knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md §C
        # and vm_ssh_readiness_probe_unroutable_from_orchestrator.md §F4).
        host = inst.metadata.get("ssh_host")
        if host and not orchestrator_can_reach(host):
            return True
        return (inst.metadata.get("snapshot_attempts") or 0) >= self._max_attempts()

    @staticmethod
    def _legacy_permit(inst: Instance) -> LifecycleActionPermit:
        return LifecycleActionPermit(
            LifecycleRouteDecision(
                job_id=str(inst.bound_to or inst.id), disposition="legacy"
            )
        )

    @asynccontextmanager
    async def lifecycle_action(
        self,
        inst: Instance,
        *,
        source: str,
    ) -> AsyncIterator[LifecycleActionPermit]:
        """Hold one command-visible job claim across VM snapshot -> delete."""

        existing = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(existing, LifecycleActionPermit):
            yield existing
            return
        job_bound = inst.metadata.get("scope") == "job" and bool(inst.bound_to)
        if self._completion_lifecycle is None or not job_bound:
            permit = self._legacy_permit(inst)
            inst.metadata["_completion_lifecycle_permit"] = permit
            try:
                yield permit
            finally:
                inst.metadata.pop("_completion_lifecycle_permit", None)
            return

        async with self._completion_lifecycle.action(
            str(inst.bound_to),
            source=f"lifecycle_vm_{source}"[:64],
            resource_kind="vm",
            resource_identity=str(inst.metadata.get("vm_uid") or inst.id),
            expected_status=str(inst.metadata.get("job_status") or "unknown"),
            expected_lane=str(inst.metadata.get("execution_lane") or "pinned"),
        ) as permit:
            inst.metadata["_completion_lifecycle_permit"] = permit
            try:
                if permit.local and self._provisioner_available():
                    if not await self._permit_external(permit):
                        yield permit
                        return
                    try:
                        async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                            identity = (
                                await self._provisioner.capture_vm_teardown_identity(
                                    str(inst.bound_to)
                                )
                            )
                    except Exception:
                        logger.exception(
                            "VM lifecycle identity capture failed for job %s",
                            inst.bound_to,
                        )
                        permit.skip("vm_identity_unknown")
                    else:
                        listed_generation = inst.metadata.get("provision_generation")
                        listed_vm_uid = inst.metadata.get("vm_uid")
                        listed_root_uid = inst.metadata.get("rootdisk_pvc_uid")
                        if (
                            (
                                listed_generation is not None
                                and identity.provision_generation != listed_generation
                            )
                            or (
                                listed_vm_uid is not None
                                and identity.vm_uid != listed_vm_uid
                            )
                            or (
                                listed_root_uid is not None
                                and identity.rootdisk_pvc_uid != listed_root_uid
                            )
                        ):
                            permit.skip("vm_identity_superseded", settled=True)
                        else:
                            inst.metadata["_completion_lifecycle_vm_identity"] = (
                                identity
                            )
                yield permit
            finally:
                inst.metadata.pop("_completion_lifecycle_permit", None)
                inst.metadata.pop("_completion_lifecycle_vm_identity", None)

    @asynccontextmanager
    async def _action_scope(
        self,
        inst: Instance,
        *,
        source: str,
    ) -> AsyncIterator[tuple[LifecycleActionPermit, bool]]:
        existing = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(existing, LifecycleActionPermit):
            yield existing, False
            return
        async with self.lifecycle_action(inst, source=source) as permit:
            yield permit, True

    async def _permit_external(self, permit: LifecycleActionPermit) -> bool:
        if not permit.local:
            return False
        if self._completion_lifecycle is None or permit.claim is None:
            return True
        return await self._completion_lifecycle.refresh(permit)

    async def record_attempt(self, inst: Instance) -> None:
        """Persist an incremented snapshot-attempt counter to the bound VM ctx."""
        if _is_stateless_instance(inst):
            return
        if self._db is None:
            return
        bound = inst.bound_to
        if not bound:
            return
        nxt = (inst.metadata.get("snapshot_attempts") or 0) + 1
        permit = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return
        try:
            if inst.metadata.get("scope") == "thread":
                await self._db.merge_thread_vm_context(
                    bound, {"snapshot_attempts": nxt}
                )
            else:
                await self._db.merge_vm_context(bound, {"snapshot_attempts": nxt})
        except Exception:
            logger.exception("Failed to record snapshot attempt for VM %s", inst.id)
            return
        if isinstance(permit, LifecycleActionPermit):
            permit.complete()

    async def give_up(self, inst: Instance, grace_s: int) -> None:
        """Escape hatch: dirty + unreachable + attempts exhausted.

        Delete the VM but KEEP its rootdisk. This fires precisely when the VM
        holds state we could not snapshot, so the disk is the only surviving
        copy — the next create reattaches it by name and the files come back.
        (Before persistent rootdisks this was a force-delete that destroyed
        them; see knowledge-base/knowledge/features/vm_persistent_rootdisk.md D2.)

        Requires the controller's ``VM_PERSISTENT_ROOTDISK``: without it the
        disk is still a dataVolumeTemplate the VM owns and cascade-deletes, so
        this degrades to the old behaviour rather than breaking.
        """
        if self._completion_lifecycle is None:
            if _is_stateless_instance(inst):
                return
            await self.delete(inst, grace_s, purge_disk=False)
            bound = inst.bound_to
            if not bound:
                return
            try:
                if inst.metadata.get("scope") == "thread":
                    await self._db.merge_thread_vm_context(bound, {"rootdisk": "kept"})
                else:
                    await self._db.merge_vm_context(bound, {"rootdisk": "kept"})
            except Exception:
                logger.exception("Failed to record kept rootdisk for VM %s", inst.id)
            return

        async with self._action_scope(inst, source="give_up") as (permit, _owns):
            if not permit.local or _is_stateless_instance(inst):
                return
            if not await self._delete_owned(inst, purge_disk=False):
                return
            bound = inst.bound_to
            if not bound:
                return
            # Marks the disk for the kept-disk GC sweep, and makes it visible in
            # the entity's context rather than only in controller logs.
            try:
                if not await self._permit_external(permit):
                    return
                if inst.metadata.get("scope") == "thread":
                    await self._db.merge_thread_vm_context(bound, {"rootdisk": "kept"})
                else:
                    await self._db.merge_vm_context(bound, {"rootdisk": "kept"})
            except Exception:
                logger.exception("Failed to record kept rootdisk for VM %s", inst.id)
                return
            if not await self._permit_external(permit):
                return
            permit.complete()

    async def snapshot(self, inst: Instance) -> str | None:
        if self._completion_lifecycle is None:
            return await self._snapshot_owned(inst)
        async with self._action_scope(inst, source="snapshot") as (permit, owns):
            if not permit.local:
                return None
            result = await self._snapshot_owned(inst)
            if owns and result is not None:
                permit.complete()
            return result

    async def _snapshot_owned(self, inst: Instance) -> str | None:
        if _is_stateless_instance(inst):
            return None
        if self._snapshot is None or not getattr(self._snapshot, "is_available", False):
            return None
        if not inst.metadata.get("ssh_host"):
            return None
        bound = inst.bound_to
        if not bound:
            return None
        is_thread = inst.metadata.get("scope") == "thread"
        owner_kind = "thread" if is_thread else "job"
        permit = inst.metadata.get("_completion_lifecycle_permit")
        identity = inst.metadata.get("_completion_lifecycle_vm_identity")
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return None
        if not isinstance(identity, VMTeardownIdentity):
            try:
                async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                    identity = await self._provisioner.capture_vm_teardown_identity(
                        bound,
                        entity_type=owner_kind,
                    )
            except Exception:
                if isinstance(permit, LifecycleActionPermit):
                    permit.skip("vm_snapshot_identity_unknown")
                return None
        if (
            not identity.ssh_host
            or not identity.ssh_port
            or not identity.ssh_host_key_fingerprint
        ):
            if isinstance(permit, LifecycleActionPermit):
                permit.skip("vm_snapshot_identity_unknown")
            return None
        async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
            disposition = await self._provisioner.revalidate_vm_teardown_identity(
                bound,
                identity,
                entity_type=owner_kind,
            )
        if disposition != "matched":
            if isinstance(permit, LifecycleActionPermit):
                permit.skip(
                    "vm_identity_superseded"
                    if disposition == "superseded"
                    else "vm_identity_unknown",
                    settled=disposition == "superseded",
                )
            return None
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return None
        try:
            exact_host = identity.ssh_host
            exact_port = identity.ssh_port
            exact_fingerprint = identity.ssh_host_key_fingerprint

            async def capture_authority() -> bool:
                if isinstance(
                    permit, LifecycleActionPermit
                ) and not await self._permit_external(permit):
                    return False
                return (
                    await self._provisioner.revalidate_vm_teardown_identity(
                        bound,
                        identity,
                        entity_type=owner_kind,
                    )
                    == "matched"
                )

            async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                ok = await self._snapshot.capture_vm_snapshot(
                    job_id=bound,
                    ssh_host=exact_host,
                    ssh_port=int(exact_port),
                    source_type="vm",
                    entity_type="threads" if is_thread else "jobs",
                    work_marker=inst.metadata.get("total_turns"),
                    expected_host_key_fingerprint=exact_fingerprint,
                    capture_authority=capture_authority,
                )
            if ok:
                if isinstance(identity, VMTeardownIdentity):
                    async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                        disposition = (
                            await self._provisioner.revalidate_vm_teardown_identity(
                                bound,
                                identity,
                                entity_type="thread" if is_thread else "job",
                            )
                        )
                    if disposition != "matched":
                        if isinstance(permit, LifecycleActionPermit):
                            permit.skip(
                                "vm_identity_superseded"
                                if disposition == "superseded"
                                else "vm_identity_unknown",
                                settled=disposition == "superseded",
                            )
                        return None
                # Success clears the escape-hatch retry counter.
                try:
                    if isinstance(
                        permit, LifecycleActionPermit
                    ) and not await self._permit_external(permit):
                        return None
                    if is_thread:
                        await self._db.merge_thread_vm_context_if_provision_generation(
                            bound,
                            identity.provision_generation,
                            {"snapshot_attempts": 0},
                        )
                    else:
                        await self._db.merge_vm_context_if_provision_generation(
                            bound,
                            identity.provision_generation,
                            {"snapshot_attempts": 0},
                        )
                except Exception:
                    logger.exception("Failed to reset attempts for VM %s", inst.id)
                if isinstance(
                    permit, LifecycleActionPermit
                ) and not await self._permit_external(permit):
                    return None
                return bound
            return None
        except Exception:
            logger.exception("Snapshot failed for VM %s (bound=%s)", inst.id, bound)
            return None

    async def restore(self, inst: Instance, snapshot_ref: str) -> None:
        # The suspension service dispatches to the right backend based
        # on the persisted ``provisioner`` field — same code path that
        # handles workspace restores. Phase 3 doesn't need its own
        # restore logic.
        if self._suspension is None or not getattr(
            self._suspension, "is_enabled", False
        ):
            return
        if inst.metadata.get("scope") == "thread":
            await self._suspension.restore_thread_workspace(snapshot_ref)
        else:
            await self._suspension.restore_workspace(snapshot_ref)

    async def signal_drain_pending(self, inst: Instance) -> None:
        """No-op: VMs have no in-pod drain hook (the management daemon
        listens on NATS but doesn't read intents from the orchestrator).
        Drift drain happens once the bound work becomes idle."""
        return None

    async def drain(self, inst: Instance, grace_s: int) -> None:
        # Drift drain wants a fresh image, so the old disk is not worth keeping.
        if _is_stateless_instance(inst):
            return
        await self.delete(inst, grace_s, purge_disk=True)

    async def delete(
        self, inst: Instance, grace_s: int, purge_disk: bool = True
    ) -> None:
        if self._completion_lifecycle is None:
            await self._delete_owned(inst, purge_disk=purge_disk)
            return
        async with self._action_scope(inst, source="delete") as (permit, _owns):
            if not permit.local:
                return
            if await self._delete_owned(inst, purge_disk=purge_disk):
                permit.complete()

    async def _delete_owned(self, inst: Instance, *, purge_disk: bool) -> bool:
        if _is_stateless_instance(inst):
            return False
        if not self._provisioner_available():
            return False
        bound = inst.bound_to
        if not bound:
            logger.debug("delete skipped: no bound id for VM %s", inst.id)
            return False
        permit = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return False
        try:
            owner_kind = "thread" if inst.metadata.get("scope") == "thread" else "job"
            identity = inst.metadata.get("_completion_lifecycle_vm_identity")
            if not isinstance(identity, VMTeardownIdentity):
                async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                    identity = await self._provisioner.capture_vm_teardown_identity(
                        bound,
                        entity_type=owner_kind,
                    )
            cleanup = await self._admit_destructive_cleanup(
                owner_kind=owner_kind,
                owner_id=bound,
                identity=identity,
                source="delete",
                purge_disk=purge_disk,
                permit=permit,
            )
            if cleanup is None:
                return False
            replayed = completed_cleanup_outcome(cleanup)
            if replayed is not None:
                outcome = VMTeardownResult(replayed, replayed == "completed")
            else:
                async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                    outcome = await self._provisioner.release_vm_captured(
                        bound,
                        identity,
                        purge_disk=purge_disk,
                        entity_type=owner_kind,
                        capture_snapshot=False,
                        **vm_cleanup_kwargs(cleanup),
                    )
            if isinstance(
                permit, LifecycleActionPermit
            ) and not await self._permit_external(permit):
                return False
            if outcome.disposition == "identity_superseded":
                if replayed is None:
                    await self._complete_destructive_cleanup(
                        cleanup, outcome="identity_superseded"
                    )
                if isinstance(permit, LifecycleActionPermit):
                    permit.skip("vm_identity_superseded", settled=True)
                return True
            if outcome.disposition == "retry_pending":
                if isinstance(permit, LifecycleActionPermit):
                    # The controller accepted the exact-generation delete and
                    # the immediate absence probe still saw it terminating.
                    # This is a conclusive incomplete attempt, not ambiguous
                    # ownership: release the term so the next lifecycle tick
                    # or authorized control can retry promptly.
                    permit.skip("vm_retirement_retry_pending", settled=True)
                return False
            if outcome.disposition == "completed":
                await self._complete_destructive_cleanup(cleanup, outcome="completed")
            return bool(outcome.deleted)
        except Exception:
            logger.exception("Failed to delete VM %s", inst.id)
            return False

    async def reap_orphans(self) -> int:
        """Backstop GC: delete VMs whose owning row is gone from BOTH tables.

        The instance path enumerates VMs FROM the jobs/threads rows
        (``_fetch_vm_rows``), so a VM whose row was deleted never surfaces as
        an ``Instance`` — it is invisible, not merely un-reapable. Only the
        backend inventory (``vm_provisioner.list_vms``) can still see it.
        This once-per-tick sweep reaps a listed VM only when
          - its name encodes a valid UUID (``agent-vm-<uuid>``),
          - it is older than the shared orphan grace (never reap an in-flight
            provision whose row hasn't landed yet), and
          - the id has NO row in ``jobs`` NOR ``threads`` — a row of ANY
            status means the instance path / dispatcher owns the VM.
        A DB error skips the VM (unknown ≠ gone), and ``list_vms() is None``
        means the inventory is unavailable (old controller, docker pool) —
        return 0, do not treat as empty. Delete via the job-keyed path: the
        controller keys teardown purely by id (``agent-vm-<id>``), and with
        the row gone the job/thread distinction is unknowable and irrelevant.

        Delete-path coverage for VMs lives in ``delete_job`` (release while
        the row exists); this sweep closes the crash/failure window. See
        knowledge-history/done/deleted_job_orphans_workspace_pod.md.
        """
        if not self._provisioner_available() or self._db is None:
            return 0
        # Rides the same tick rather than adding a second scheduler.
        await self.purge_kept_disks()
        try:
            async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                vms = await self._provisioner.list_vms(include_teardown_identity=True)
        except Exception:
            logger.exception("VM orphan sweep: list failed")
            return 0
        if not vms:
            return 0
        reaped = 0
        for vm in vms:
            if not isinstance(vm, dict):
                continue
            entity_id = vm.get("entity_id") or ""
            try:
                uuid.UUID(entity_id)
            except (ValueError, AttributeError, TypeError):
                continue  # not one of ours — never touch it
            age = _vm_age_seconds(vm.get("created_at"))
            if age is None or age < orphan_grace_seconds():
                continue
            try:
                async with self._db.acquire() as conn:
                    row_exists = await conn.fetchval(
                        """
                        SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1::uuid)
                            OR EXISTS (SELECT 1 FROM threads WHERE id = $1::uuid)
                        """,
                        entity_id,
                    )
            except Exception:
                logger.exception(
                    "VM orphan sweep: row lookup failed for %s — skipping",
                    entity_id,
                )
                continue
            if row_exists:
                continue
            generation = vm.get("provision_generation")
            vm_uid = vm.get("vm_uid")
            rootdisk_uid = vm.get("rootdisk_pvc_uid")
            if (
                not isinstance(generation, str)
                or not isinstance(vm_uid, str)
                or not isinstance(rootdisk_uid, str)
            ):
                logger.warning(
                    "VM orphan sweep: inventory lacks authenticated identity "
                    "for %s — preserving it",
                    entity_id,
                )
                continue
            identity = VMTeardownIdentity(
                provision_generation=generation,
                vm_uid=vm_uid,
                rootdisk_pvc_uid=rootdisk_uid,
            )
            try:
                cleanup = await self._admit_destructive_cleanup(
                    owner_kind="job",
                    owner_id=entity_id,
                    identity=identity,
                    source="orphan_delete",
                    purge_disk=True,
                )
                if cleanup is None:
                    continue
                replayed = completed_cleanup_outcome(cleanup)
                if replayed is not None:
                    outcome = VMTeardownResult(replayed, replayed == "completed")
                else:
                    async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                        outcome = await self._provisioner.delete_orphan_vm_captured(
                            entity_id,
                            identity,
                            purge_disk=True,
                            **vm_cleanup_kwargs(cleanup),
                        )
                if outcome.disposition in {
                    "completed",
                    "identity_superseded",
                }:
                    await self._complete_destructive_cleanup(
                        cleanup, outcome=outcome.disposition
                    )
                if outcome.deleted:
                    reaped += 1
                    logger.warning(
                        "Orphan VM reaped: %s (no jobs/threads row, age %.0fs)",
                        vm.get("vm_name") or entity_id,
                        age,
                    )
            except Exception:
                logger.exception("Orphan VM delete failed: %s", entity_id)
        return reaped

    async def purge_kept_disks(self) -> int:
        """Reclaim rootdisks kept for a recovery that is never coming.

        Layer 2 of the rootdisk GC (knowledge-base/knowledge/features/vm_persistent_rootdisk.md
        D4). The normal lifecycle is covered by layer 1: a terminal
        release/delete purges the disk with its VM. This catches the gap —
        a job that was crash-recovered (disk kept) and then cancelled or
        failed *without* a live VM, so no delete ever ran and 20 Gi sits
        there indefinitely.

        **Jobs only, deliberately.** A thread's terminal status is ``ended``,
        which is also exactly the state a suspended-but-resumable session sits
        in — a kept disk there is the session's workspace waiting for a resume,
        not a leak. Session disks are reclaimed by ``release_thread_vm`` on an
        explicit end.

        The marker is cleared only after a delete the provisioner accepted, so
        a failed purge stays on the worklist instead of being silently
        forgotten. A DB error reclaims nothing: unknown is not terminal.
        """
        if not self._provisioner_available() or self._db is None:
            return 0
        try:
            async with self._db.acquire() as conn:
                if self._completion_lifecycle is None:
                    rows = await conn.fetch(
                        """
                        SELECT id::text AS id, execution_lane
                        FROM jobs
                        WHERE status = ANY($1::text[])
                          AND context->'vm'->>'rootdisk' = 'kept'
                          AND execution_lane IS DISTINCT FROM 'stateless'
                          AND NOT EXISTS (SELECT 1 FROM vm_idle_operations retained
                              WHERE retained.owner_kind='job' AND retained.owner_id=jobs.id
                                AND retained.provision_generation::text=jobs.context->'vm'->>'provision_generation'
                                AND retained.pvc_uid::text=jobs.context->'vm'->>'rootdisk_pvc_uid'
                                AND retained.storage_disposition='retention_unknown')
                        LIMIT 100
                        """,
                        list(_TERMINAL_JOB_STATUSES),
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT id::text AS id, status::text AS status,
                               execution_lane,
                               context->'vm' AS vm_context
                        FROM jobs
                        WHERE status = ANY($1::text[])
                          AND context->'vm'->>'rootdisk' = 'kept'
                          AND execution_lane IS DISTINCT FROM 'stateless'
                          AND NOT EXISTS (SELECT 1 FROM vm_idle_operations retained
                              WHERE retained.owner_kind='job' AND retained.owner_id=jobs.id
                                AND retained.provision_generation::text=jobs.context->'vm'->>'provision_generation'
                                AND retained.pvc_uid::text=jobs.context->'vm'->>'rootdisk_pvc_uid'
                                AND retained.storage_disposition='retention_unknown')
                        LIMIT 100
                        """,
                        list(_TERMINAL_JOB_STATUSES),
                    )
        except Exception:
            logger.exception("Kept-disk sweep: query failed")
            return 0

        purged = 0
        for row in rows or []:
            # SQL owns the normal filter; retain an exact application-side
            # belt for mocked/stale readers and future query refactors.
            if row.get("execution_lane") == "stateless":
                continue
            job_id = row["id"] if isinstance(row, dict) else row.get("id")
            if not job_id:
                continue
            if self._completion_lifecycle is None:
                try:
                    # Idempotent: a VM that is already gone 404s, which the
                    # provisioner treats as success, and the disk still goes.
                    async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                        identity = await self._provisioner.capture_vm_teardown_identity(
                            str(job_id), entity_type="job"
                        )
                        cleanup = await self._admit_destructive_cleanup(
                            owner_kind="job",
                            owner_id=str(job_id),
                            identity=identity,
                            source="kept_disk",
                            purge_disk=True,
                        )
                        if cleanup is None:
                            continue
                        replayed = completed_cleanup_outcome(cleanup)
                        if replayed is not None:
                            outcome = VMTeardownResult(
                                replayed, replayed == "completed"
                            )
                        else:
                            outcome = await self._provisioner.release_vm_captured(
                                str(job_id),
                                identity,
                                purge_disk=True,
                                entity_type="job",
                                capture_snapshot=False,
                                **vm_cleanup_kwargs(cleanup),
                            )
                    if outcome.disposition in {
                        "completed",
                        "identity_superseded",
                    }:
                        await self._complete_destructive_cleanup(
                            cleanup, outcome=outcome.disposition
                        )
                    if not outcome.deleted:
                        continue
                    await self._db.merge_vm_context(job_id, {"rootdisk": None})
                    purged += 1
                    logger.info("Kept rootdisk reclaimed for terminal job %s", job_id)
                except Exception:
                    logger.exception("Kept-disk purge failed for job %s", job_id)
                continue
            vm_context = _coerce_jsonb(row.get("vm_context"))
            inst = Instance(
                kind=self.kind,
                id=str(vm_context.get("vm_name") or f"vm-job-{str(job_id)[:12]}"),
                bound_to=str(job_id),
                metadata={
                    "scope": "job",
                    "job_status": str(row.get("status")),
                    "execution_lane": str(row.get("execution_lane") or "pinned"),
                    "provision_generation": vm_context.get("provision_generation"),
                    "vm_uid": vm_context.get("vm_uid"),
                    "rootdisk_pvc_uid": vm_context.get("rootdisk_pvc_uid"),
                },
            )
            async with self.lifecycle_action(inst, source="kept_disk") as permit:
                if not permit.local or not await self._permit_external(permit):
                    continue
                identity = inst.metadata.get("_completion_lifecycle_vm_identity")
                if not isinstance(identity, VMTeardownIdentity):
                    continue
                try:
                    cleanup = await self._admit_destructive_cleanup(
                        owner_kind="job",
                        owner_id=str(job_id),
                        identity=identity,
                        source="kept_disk",
                        purge_disk=True,
                        permit=permit,
                    )
                    if cleanup is None:
                        continue
                    # Idempotent: a VM that is already gone 404s, which the
                    # provisioner treats as success, and the disk still goes.
                    replayed = completed_cleanup_outcome(cleanup)
                    if replayed is not None:
                        outcome = VMTeardownResult(replayed, replayed == "completed")
                    else:
                        async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                            outcome = await self._provisioner.release_vm_captured(
                                str(job_id),
                                identity,
                                purge_disk=True,
                                **vm_cleanup_kwargs(cleanup),
                            )
                    if outcome.disposition in {
                        "completed",
                        "identity_superseded",
                    }:
                        await self._complete_destructive_cleanup(
                            cleanup, outcome=outcome.disposition
                        )
                    if not await self._permit_external(permit):
                        continue
                    if outcome.disposition == "identity_superseded":
                        permit.skip("vm_identity_superseded", settled=True)
                        continue
                    if outcome.disposition == "retry_pending":
                        permit.skip("vm_retirement_retry_pending", settled=True)
                        continue
                    if not outcome.deleted:
                        continue
                    await self._db.merge_vm_context(job_id, {"rootdisk": None})
                    if not await self._permit_external(permit):
                        continue
                    permit.complete()
                    purged += 1
                    logger.info("Kept rootdisk reclaimed for terminal job %s", job_id)
                except Exception:
                    logger.exception("Kept-disk purge failed for job %s", job_id)
        return purged

    async def _admit_destructive_cleanup(
        self,
        *,
        owner_kind: str,
        owner_id: str,
        identity: VMTeardownIdentity,
        source: str,
        purge_disk: bool,
        permit: LifecycleActionPermit | None = None,
    ) -> Any | None:
        """Serialize an exact VM delete/prune before controller side effects."""

        if purge_disk and owner_kind == "job":
            from orchestrator.services.vm_idle_lifecycle import (
                retained_terminal_rootdisk,
            )

            if await retained_terminal_rootdisk(
                self._db,
                job_id=owner_id,
                generation=identity.provision_generation,
                pvc_uid=identity.rootdisk_pvc_uid,
            ):
                logger.info(
                    "Kept rootdisk purge held for terminal review job %s", owner_id
                )
                return None

        try:
            parsed_owner_id = UUID(str(owner_id))
        except (TypeError, ValueError, AttributeError):
            # Lifecycle's production owners are UUID-backed rows. Keeping a
            # deterministic fallback makes old test doubles and imported rows
            # serialize consistently without weakening real UUID ownership.
            parsed_owner_id = uuid5(
                NAMESPACE_URL, f"workspace-cleanup-owner:{owner_kind}:{owner_id}"
            )
        try:
            parsed_pvc_uid = (
                UUID(str(identity.rootdisk_pvc_uid))
                if identity.rootdisk_pvc_uid is not None
                else None
            )
        except (TypeError, ValueError, AttributeError):
            parsed_pvc_uid = None
        request_id = uuid5(
            NAMESPACE_URL,
            "workspace-cleanup:"
            f"{source}:{owner_kind}:{owner_id}:{identity.provision_generation}:"
            f"{identity.vm_uid}:{identity.rootdisk_pvc_uid}",
        )
        resource_intent = {
            "owner_kind": owner_kind,
            "owner_id": str(parsed_owner_id),
            "provision_generation": identity.provision_generation,
            "vm_uid": identity.vm_uid or "",
            "pvc_uid": str(parsed_pvc_uid or ""),
            "purge_disk": bool(purge_disk),
            "resource": "vm_workspace",
            "source": f"lifecycle_vm_{source}"[:64],
        }
        try:
            cleanup = await self._workspace_recovery_store.acquire_cleanup_permit(
                owner_kind=owner_kind,
                owner_id=parsed_owner_id,
                pvc_uid=parsed_pvc_uid,
                request_id=request_id,
                source=f"lifecycle_vm_{source}"[:64],
                intent_digest=cleanup_intent_digest(resource_intent),
            )
        except Exception:
            logger.exception(
                "VM cleanup admission unavailable for %s %s — preserving workspace",
                owner_kind,
                owner_id,
            )
            if isinstance(permit, LifecycleActionPermit):
                permit.skip("workspace_cleanup_admission_unavailable", settled=True)
            return None
        if cleanup.allowed:
            bound = bind_vm_cleanup_permit(
                cleanup, request_id=request_id, intent=resource_intent
            )
            if owner_kind == "job":
                from orchestrator.services.vm_workspace_recovery_store import (
                    prepare_vm_cleanup_resource,
                )

                await prepare_vm_cleanup_resource(
                    self._workspace_recovery_store,
                    bound,
                )
            return bound
        logger.info(
            "VM cleanup held for %s %s (%s)",
            owner_kind,
            owner_id,
            cleanup.reason or "workspace_recovery_unresolved",
        )
        if isinstance(permit, LifecycleActionPermit):
            permit.skip("workspace_recovery_hold", settled=True)
        return None

    async def _complete_destructive_cleanup(
        self, cleanup: Any, *, outcome: str
    ) -> None:
        admission_id = getattr(cleanup, "admission_id", None)
        if admission_id is None:
            return
        try:
            from orchestrator.services.vm_workspace_recovery_store import (
                complete_vm_cleanup_permit,
            )

            await complete_vm_cleanup_permit(
                self._workspace_recovery_store,
                cleanup,
                outcome=outcome,
                provisioner=self._provisioner,
            )
        except Exception:
            # Physical deletion does not settle the durable permit or charge.
            # Preserve the owner's context and incomplete lifecycle action on
            # native guard/connection failures as well as domain refusals.
            logger.exception(
                "VM cleanup admission %s could not be completed", admission_id
            )
            raise

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _provisioner_available(self) -> bool:
        # VMProvisioner deliberately closes ``is_available`` in external mode:
        # that property is the create/admission gate.  The lifecycle manager is
        # different -- it must keep observing and retiring exact-generation VMs
        # which predate external-create containment.  Use the provisioner's
        # explicitly narrower existing-runtime capability here; creation callers
        # continue to gate on ``is_available`` at their own call sites.
        return bool(getattr(self._provisioner, "lifecycle_available", False))

    async def _fetch_vm_rows(self) -> list[dict[str, Any]]:
        """Pull both job-bound and thread-bound VMs.

        VMs aren't a fleet-wide K8s object you can list_namespaced_pod;
        each backend (NATS / HTTP controller / KubeVirt / Docker)
        tracks them differently. The orchestrator's authoritative view
        is the ``vm`` JSONB on jobs and threads, so we read from there.
        """
        if self._db is None:
            return []
        rows: list[dict[str, Any]] = []
        try:
            async with self._db.acquire() as conn:
                job_rows = await conn.fetch(
                    """
                    SELECT id, status, context, updated_at, freeze_data,
                           execution_lane,
                           (assigned_agent_id IS NULL) AS unassigned,
                           (freeze_data IS NULL) AS freeze_free
                    FROM jobs
                    WHERE context->'vm' IS NOT NULL
                      AND context->'vm' <> '{}'::jsonb
                    """
                )
                thread_rows = await conn.fetch(
                    """
                    SELECT id, status, execution_lane, total_turns, metadata
                    FROM threads
                    WHERE metadata->'vm' IS NOT NULL
                      AND metadata->'vm' <> '{}'::jsonb
                    """
                )
        except Exception:
            logger.exception("Failed to query VMs for lifecycle")
            return []

        for r in job_rows:
            ctx = _coerce_jsonb(r.get("context"))
            vm_ctx = _coerce_jsonb(ctx.get("vm"))
            if not vm_ctx:
                continue
            # Mirror get_dispatchable_jobs: created/paused + unassigned +
            # freeze-free == the dispatcher owns this VM's bring-up. Carried to
            # is_reapable so the reaper hands off a resuming VM (see there).
            # Deliberately NOT mirrored: the operator pause hold. A held job
            # is waiting for its explicit resume, which must land on this VM.
            dispatchable = (
                r.get("status") in ("created", "paused")
                and bool(r.get("unassigned"))
                and bool(r.get("freeze_free"))
            )
            rows.append(
                {
                    "scope": "job",
                    "bound_id": str(r["id"]),
                    "owner_status": r.get("status"),
                    "owner_updated_at": r.get("updated_at"),
                    "execution_lane": r.get("execution_lane"),
                    "job_dispatchable": dispatchable,
                    # Carried for the infra_transient reap carve-out: a job
                    # paused for a DB-blip retry must keep its VM, or the retry
                    # resumes into an empty workspace. (Defect 1b)
                    "job_freeze_data": _coerce_jsonb(r.get("freeze_data")),
                    "vm_ctx": vm_ctx,
                    "ide_session_status": _coerce_jsonb(ctx.get("ide_session")).get(
                        "status"
                    ),
                    "snapshot_status": _coerce_jsonb(ctx.get("snapshot")).get("status"),
                }
            )
        for r in thread_rows:
            md = _coerce_jsonb(r.get("metadata"))
            vm_ctx = _coerce_jsonb(md.get("vm"))
            if not vm_ctx:
                continue
            rows.append(
                {
                    "scope": "thread",
                    "bound_id": str(r["id"]),
                    "owner_status": r.get("status"),
                    "execution_lane": r.get("execution_lane"),
                    "vm_ctx": vm_ctx,
                    "ide_session_status": _coerce_jsonb(md.get("ide_session")).get(
                        "status"
                    ),
                    "total_turns": r.get("total_turns") or 0,
                    "snapshot_status": _coerce_jsonb(md.get("snapshot")).get("status"),
                }
            )
        return rows

    def _row_to_instance(self, row: dict[str, Any]) -> Instance | None:
        scope = row["scope"]
        bound_id = row["bound_id"]
        vm_ctx = row["vm_ctx"]
        # Already torn down / restorable-on-demand, or parked after a
        # provisioning failure → not a live instance the reconciler should act
        # on. Skip so reap/drift/crash don't loop on a VM whose teardown is
        # already in flight (_TORN_DOWN_VM_STATUSES) or that the dispatcher is
        # deliberately holding parked (_PARKED_VM_STATUSES).
        if vm_ctx.get("status") in _TORN_DOWN_VM_STATUSES | _PARKED_VM_STATUSES:
            return None
        # Identity: prefer a backend-native id when present, otherwise
        # synthesize one from scope+bound_id so the reconciler can
        # log/distinguish even before the backend assigns one.
        vm_native_id = (
            vm_ctx.get("vm_name") or vm_ctx.get("name") or vm_ctx.get("ssh_host")
        )
        inst_id = vm_native_id or f"vm-{scope}-{bound_id[:12]}"
        version = _extract_sha(vm_ctx.get("vm_image"))
        metadata: dict[str, Any] = {
            "scope": scope,
            "execution_lane": row.get("execution_lane"),
            # Backend-native id (None if only the synthesized fallback exists) —
            # the live-child guard matches critic subjobs on it. A critic
            # inherits the parent's context.vm, so its native id equals this
            # one; a delegation child on its own VM does not.
            "vm_native_id": vm_native_id,
            "vm_status": vm_ctx.get("status"),
            "ssh_host": vm_ctx.get("ssh_host") or vm_ctx.get("host"),
            "ssh_port": vm_ctx.get("ssh_port") or vm_ctx.get("port"),
            "provisioner": vm_ctx.get("provisioner"),
            "ide_session_status": row.get("ide_session_status"),
            # Reap-path inputs (mirror WorkspaceInstanceManager).
            "last_snapshot_turns": vm_ctx.get("last_snapshot_turns"),
            "snapshot_attempts": vm_ctx.get("snapshot_attempts") or 0,
            "snapshot_status": row.get("snapshot_status"),
        }
        if self._completion_lifecycle is not None:
            metadata.update(
                {
                    "provision_generation": vm_ctx.get("provision_generation"),
                    "vm_uid": vm_ctx.get("vm_uid"),
                    "rootdisk_pvc_uid": vm_ctx.get("rootdisk_pvc_uid"),
                    "identity_authenticated": (
                        vm_ctx.get("identity_authenticated") is True
                    ),
                    "identity_provision_generation": vm_ctx.get(
                        "identity_provision_generation"
                    ),
                }
            )
        if scope == "job":
            metadata["job_status"] = row.get("owner_status")
            metadata["job_updated_at"] = row.get("owner_updated_at")
            metadata["job_dispatchable"] = bool(row.get("job_dispatchable"))
            metadata["job_freeze"] = row.get("job_freeze_data") or {}
        else:
            metadata["thread_status"] = row.get("owner_status")
            metadata["total_turns"] = row.get("total_turns") or 0
        return Instance(
            kind=self.kind,
            id=inst_id,
            version=version,
            bound_to=bound_id,
            metadata=metadata,
        )

    async def _live_shared_child_exists(
        self, parent_job_id: str, vm_native_id: str | None
    ) -> bool:
        """True if a non-terminal child job shares this parent's VM.

        Mirrors ``WorkspaceInstanceManager._live_shared_child_exists``: a critic
        verification subjob inherits the parent's ``context.vm`` at spawn
        (``_trigger_verification_on_complete`` copies it) and SSHes into the
        *parent's* VM. Match on the VM's native id — the same
        ``vm_name / name / ssh_host`` COALESCE ``_row_to_instance`` uses for the
        instance id — which the critic's inherited copy reproduces exactly,
        whereas a delegation child (its own VM) does not, keeping the guard
        narrow. Fail-safe: on a DB error, assume shared (do not reap).
        """
        if self._db is None or not vm_native_id:
            return False
        try:
            async with self._db.acquire() as conn:
                return bool(
                    await conn.fetchval(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM jobs
                            WHERE parent_job_id = $1::uuid
                              AND status NOT IN ('completed', 'failed', 'cancelled')
                              AND COALESCE(
                                    context->'vm'->>'vm_name',
                                    context->'vm'->>'name',
                                    context->'vm'->>'ssh_host'
                                  ) = $2
                        )
                        """,
                        parent_job_id,
                        vm_native_id,
                    )
                )
        except Exception:
            logger.exception(
                "Failed to check live shared child for VM %s (parent %s); "
                "assuming shared — not reaping",
                vm_native_id,
                parent_job_id,
            )
            return True
