"""WorkspaceInstanceManager — Phase 2 stateful manager.

Wraps ``ContainerProvisioner``, ``WorkspaceSuspensionService``, and
``SnapshotService`` to surface workspace pods to the unified lifecycle
reconciler. Implements ``StatefulInstanceManager`` so the reconciler
calls ``snapshot()`` before any drift-driven ``drain()`` — the snapshot
ends up in S3, the pod gets deleted, and on next dispatch
``WorkspaceSuspensionService.restore_*`` rehydrates a fresh-version pod
from the same S3 reference.

Phase 2a scope: drift detection + snapshot/drain integration.
Phase 2b adds crash recovery for ``Unknown`` / ``Failed`` workspace
pods (the gap in ``knowledge-base/knowledge/issues/stuck_thread_workspace_pods.md``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from orchestrator.services.lifecycle.types import Instance
from orchestrator.services.container_provisioner import (
    WorkspaceCleanupOutcome,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.completion_lifecycle import (
    CompletionLifecycleOwnership,
    LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS,
    LifecycleActionPermit,
    LifecycleRouteDecision,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner

logger = logging.getLogger(__name__)


_LABEL_SELECTOR = "srw.io/component=agent-workspace"

# Sentinel distinguishing "the bound-row lookup failed" (DB error / no DB) from
# "the lookup succeeded and found no row". The distinction matters: a missing
# row means the job/thread was deleted and the pod is an orphan we may reap
# (age-gated), while a failed lookup means we know nothing and must not act.
# See knowledge-history/done/deleted_job_orphans_workspace_pod.md.
_FETCH_FAILED = object()

# 'reviewing' is the verification-enabled twin of 'pending_review'
# (determine_job_status sets it on a critic-enabled completion freeze). The
# parent has frozen, so by *status* it is as quiescent as 'pending_review' and
# belongs in the idle set. The catch: a critic subjob SSHes into the *parent's*
# live workspace pod (shared by design, to read the parent's output/), so
# reaping on status alone pulls the pod out from under a live critic → headless
# Service with zero endpoints → the critic's next SSH fails NXDOMAIN and the
# whole review dies. The is_idle/is_reapable predicates below therefore gate on
# ``has_live_shared_child`` (a non-terminal child bound to this same pod); the
# status set stays optimistic and the guard handles the dependency precisely.
# See knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
_IDLE_JOB_STATUSES = frozenset(
    {"paused", "pending_review", "reviewing", "waiting_for_reply"}
)
_IDLE_THREAD_STATUSES = frozenset({"ended"})

# Terminal = bound work is finished; nothing to preserve beyond an existing
# snapshot. Reapable = the pod is no longer needed at all — the union of
# suspendable-idle (snapshot + free) and terminal (clean up).
#
# READ THIS BEFORE REUSING THESE SETS FOR STORAGE: "terminal" is a statement
# about the POD, not about the VOLUME. A thread reaches 'ended' on an ordinary
# 30-minute idle timeout (the agent's own idle-archive handler flips it) and is
# still RESUMABLE from there — ``resume_thread`` in main.py requires exactly
# ``status == "ended"`` to bring the session back. So 'ended' licenses a pod
# teardown (that is how idle suspend saves money) and MUST NEVER license a PVC
# delete: once sessions are PVC-backed, reclaiming on 'ended' would destroy the
# user's working tree on an idle timeout — strictly worse than the emptyDir
# behavior the PVC replaces. The volume-side predicate is
# ``_is_volume_reclaimable``; keep the two apart.
# See knowledge-base/knowledge/features/workspace_pvc_backed_migration.md ("thread **deleted**, not
# merely *ended*") and workspace_pvc_branch_a_implementation.md.
_TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})
_TERMINAL_THREAD_STATUSES = frozenset({"ended"})
_REAPABLE_JOB_STATUSES = _IDLE_JOB_STATUSES | _TERMINAL_JOB_STATUSES
_REAPABLE_THREAD_STATUSES = _IDLE_THREAD_STATUSES | _TERMINAL_THREAD_STATUSES

# PVC names the backstop sweep owns, by owner kind. The create side is the
# source of truth — ``_pvc_name_for`` in container_provisioner.py (workspace
# pod, both kinds) and the session-agent claim in agent_provisioner.py — so
# these prefixes must track those two spellings. Every name is
# ``<prefix><owner_id[:12]>``. Anything NOT matched here (the shared
# ``srw-workspace`` agent-scratch claim, unlabeled claims, foreign names) is out
# of scope and is never deleted by this module.
_JOB_PVC_PREFIX = "pvc-workspace-"
_SESSION_PVC_PREFIXES = ("pvc-ws-thread-", "pvc-agent-s-")
# Owner ids are truncated to 12 chars in PVC names (matching the pod names), so
# a name-derived thread reference is a prefix, not a key.
_PVC_ID_PREFIX_LEN = 12


def expected_workspace_shas() -> set[str]:
    """SHAs from the configured workspace image tag.

    Reads ``WORKSPACE_IMAGE`` and extracts the suffix from a
    ``...:sha-<hash>`` tag. Returns empty set for ``:latest`` or
    semver-style tags — the reconciler then skips drift checks.
    """
    shas: set[str] = set()
    tag = os.environ.get("WORKSPACE_IMAGE", "")
    if ":sha-" in tag:
        shas.add(tag.rsplit(":sha-", 1)[-1])
    return shas


def orphan_grace_seconds() -> float:
    """Minimum instance age before a missing-row pod/VM is treated as an orphan.

    Must comfortably exceed the create-instance → persist-context window during
    provisioning (seconds); anything shorter risks reaping an in-flight
    instance whose row simply hasn't landed yet. Shared by the workspace
    missing-row reap and the VM orphan sweep."""
    try:
        return float(os.environ.get("WORKSPACE_ORPHAN_GRACE_SECONDS", "900"))
    except ValueError:
        return 900.0


def _pod_age_seconds(pod: Any) -> float | None:
    """Pod age from creationTimestamp, or None when it can't be determined."""
    try:
        created = pod.metadata.creation_timestamp
        if not isinstance(created, datetime):
            return None
        return max(0.0, (datetime.now(timezone.utc) - created).total_seconds())
    except Exception:
        return None


def paused_grace_seconds() -> float:
    """Warm-grace window before a ``paused`` job's workspace becomes reapable.

    A pause is frequently a human-wait (sudo/VM-upgrade approval: 24 h TTL;
    review pauses) — reaping on the very next tick destroys the workspace
    minutes into that window (see
    knowledge-base/knowledge/issues/vm_upgrade_pause_workspace_reaped_before_approval.md). Keep it
    warm for the grace so a fast decision resumes losslessly; a slow one pays
    a snapshot-restore. Defaults to the suspension sweep's
    ``WORKSPACE_IDLE_TIMEOUT`` (minutes, default 30) so the graceful
    snapshot-then-free path gets first claim on the workspace.
    """
    try:
        return float(os.environ["WORKSPACE_PAUSED_REAP_GRACE_S"])
    except (KeyError, ValueError):
        pass
    try:
        return float(os.environ.get("WORKSPACE_IDLE_TIMEOUT", "30")) * 60.0
    except ValueError:
        return 1800.0


def _paused_age_seconds(metadata: dict[str, Any]) -> float | None:
    """Seconds since the bound job's last row update (pause-time proxy).

    ``jobs.updated_at`` is bumped by the status flip that paused the job;
    later bookkeeping merges bump it too, which only ever EXTENDS the grace
    (activity-bumped, Coder-style). None when the timestamp is unavailable.
    """
    ts = metadata.get("job_updated_at")
    if not isinstance(ts, datetime):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def paused_within_grace(metadata: dict[str, Any]) -> bool:
    """True while a ``paused`` job's workspace is still inside the warm grace.

    An unknown pause age counts as inside the grace (conservative: never
    destroy on missing data — mirrors the ``_FETCH_FAILED`` stance).
    """
    if metadata.get("job_status") != "paused":
        return False
    age = _paused_age_seconds(metadata)
    return age is None or age < paused_grace_seconds()


def infra_transient_retry_pending(metadata: dict[str, Any]) -> bool:
    """True while a job paused for a transient-infra retry still owns its workspace.

    Pausing a job does NOT keep its workspace: a paused-and-frozen job is not
    dispatchable, so ``is_idle``/``is_reapable`` classify it as idle and the
    reaper collects it once ``paused_within_grace`` expires. Without this
    carve-out the whole "keep the VM and resume in place" design is a no-op —
    the retry would come back to a reprovisioned, empty workspace, which is
    exactly the loss this fix exists to prevent.

    Bounded on two independent axes so a VM can never be held indefinitely:

      * ``freeze_type == "infra_transient"`` — only this freeze qualifies;
      * ``next_retry_at`` is still in the future — and the backoff caps that at
        one hour, so the hold is short even in the worst case.

    Past the attempt ceiling the job is terminally ``failed``, not ``paused``,
    so it stops matching here and the normal terminal reap collects it. That is
    why no attempt counter is needed in this predicate.

    knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 1b)
    """
    freeze = metadata.get("job_freeze")
    if not isinstance(freeze, dict):
        return False
    if freeze.get("freeze_type") != "infra_transient":
        return False
    raw = freeze.get("next_retry_at")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        next_retry = datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        # An unparseable timestamp must not pin the VM forever.
        return False
    if next_retry.tzinfo is None:
        next_retry = next_retry.replace(tzinfo=timezone.utc)
    return next_retry > datetime.now(timezone.utc)


def _pod_volume_is_ephemeral(pod: Any) -> bool:
    """True if the pod's workspace-data volume is emptyDir (vs a PVC).

    Reads the pod spec, not a flag, so a mixed fleet (some emptyDir, some PVC)
    reconciles correctly mid-cutover. ``workspace-data`` is the volume name
    ``container_provisioner._build_pod_manifest`` uses for BOTH owner kinds —
    job and session execution pods come off the same builder — so a PVC-backed
    session pod is correctly reported non-ephemeral here, which is what makes it
    skip the reap-and-restore handoff in ``delete()`` (an S3 extract could
    otherwise roll newer on-volume files back).

    Defaults to True (ephemeral) when the volume can't be read — matches the
    current fleet default and keeps the reaper conservative.
    """
    try:
        for vol in pod.spec.volumes or []:
            if getattr(vol, "name", None) == "workspace-data":
                if getattr(vol, "persistent_volume_claim", None) is not None:
                    return False
                return getattr(vol, "empty_dir", None) is not None
    except Exception:
        pass
    return True


def _is_stateless_thread_instance(inst: Instance) -> bool:
    """Whether ``inst`` belongs to the acknowledged stateless session lane.

    The generic lifecycle reconciler predates stateless session ownership.  It
    must never snapshot or tear down one of those workspaces: terminal/loss
    retirement owns the physical instance and records the durable acknowledgement
    that makes cleanup safe.  Match both the thread ownership label and the exact
    lane value so job workspaces and legacy/future lanes retain their established
    lifecycle behavior.
    """
    labels = inst.metadata.get("labels")
    return (
        isinstance(labels, dict)
        and "srw/thread-id" in labels
        and inst.metadata.get("execution_lane") == "stateless"
    )


class WorkspaceInstanceManager:
    """Lifecycle manager for the workspace kind."""

    kind = "workspace"

    def __init__(
        self,
        container_provisioner: Any,
        suspension_service: Any,
        snapshot_service: Any,
        db: Any,
        label_selector: str = _LABEL_SELECTOR,
        *,
        completion_commands_enabled: bool = False,
        completion_router: Any | None = None,
    ):
        # Loose typing matches the agent manager — production passes the
        # singleton modules; tests pass mocks. Only the methods used here
        # need to exist on the underlying objects.
        self._provisioner = container_provisioner
        self._suspension = suspension_service
        self._snapshot = snapshot_service
        self._db = db
        self._label_selector = label_selector
        # Constructor-gated so flag-off callers preserve the exact legacy path,
        # including zero reads of Gate-3 relations and zero reserved metadata.
        self._completion_commands_enabled = completion_commands_enabled
        self._completion_lifecycle = (
            CompletionLifecycleOwnership(db, completion_router)
            if completion_commands_enabled and completion_router is not None
            else None
        )
        # Reachability probe cache: pod_ip -> (probed_at, ok). Single
        # orchestrator process, so a plain dict is sufficient.
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
        return expected_workspace_shas()

    async def list_instances(self) -> list[Instance]:
        if not self._provisioner_ready():
            return []

        pods = await self._list_pods()
        instances: list[Instance] = []
        for pod in pods:
            labels = pod.metadata.labels or {}
            # Job workspaces carry srw/job-id; thread (session) workspaces
            # carry srw/thread-id. Check thread-id first so we route to the
            # right table.
            thread_id = labels.get("srw/thread-id")
            job_id = labels.get("srw/job-id") if not thread_id else None
            metadata: dict[str, Any] = {
                "pod_phase": pod.status.phase,
                "labels": dict(labels),
                "kind_label": labels.get("srw/component"),
                "volume_ephemeral": _pod_volume_is_ephemeral(pod),
                "pod_age_s": _pod_age_seconds(pod),
            }
            # Exact-runtime cleanup is mandatory in both completion-owned and
            # legacy lifecycle modes. A deterministic Pod name is never an
            # acceptable teardown target because a replacement may already
            # own it by the time deletion runs.
            pod_uid = getattr(pod.metadata, "uid", None)
            if isinstance(pod_uid, str) and pod_uid:
                metadata["pod_uid"] = pod_uid
            if thread_id:
                row = await self._fetch_thread(thread_id)
                if row is _FETCH_FAILED:
                    pass  # unknown state — leave metadata bare, never reap
                elif row is None:
                    metadata["bound_row_missing"] = True
                else:
                    metadata["thread_status"] = row.get("status")
                    metadata["execution_lane"] = row.get("execution_lane")
                    metadata["total_turns"] = row.get("total_turns") or 0
                    md = row.get("metadata") or {}
                    if isinstance(md, str):
                        try:
                            md = json.loads(md)
                        except (json.JSONDecodeError, ValueError):
                            md = {}
                    ws = md.get("workspace_container") or {}
                    metadata["workspace_status"] = ws.get("status")
                    metadata["pod_ip"] = ws.get("pod_ip")
                    metadata["last_snapshot_turns"] = ws.get("last_snapshot_turns")
                    metadata["snapshot_attempts"] = ws.get("snapshot_attempts") or 0
                    snap = md.get("snapshot") or {}
                    metadata["snapshot_status"] = snap.get("status")
            elif job_id:
                row = await self._fetch_job(job_id)
                if row is _FETCH_FAILED:
                    pass  # unknown state — leave metadata bare, never reap
                elif row is None:
                    metadata["bound_row_missing"] = True
                else:
                    metadata["job_status"] = row.get("status")
                    metadata["job_updated_at"] = row.get("updated_at")
                    ctx = row.get("context") or {}
                    if isinstance(ctx, str):
                        try:
                            ctx = json.loads(ctx)
                        except (json.JSONDecodeError, ValueError):
                            ctx = {}
                    ws_ctx = ctx.get("workspace_container") or {}
                    metadata["workspace_status"] = ws_ctx.get("status")
                    metadata["pod_ip"] = ws_ctx.get("pod_ip")
                    metadata["snapshot_attempts"] = ws_ctx.get("snapshot_attempts") or 0
                    snap = ctx.get("snapshot") or {}
                    metadata["snapshot_status"] = snap.get("status")
                    if self._completion_lifecycle is not None:
                        metadata["execution_lane"] = (
                            row.get("execution_lane") or "pinned"
                        )
                        decision = await self._completion_lifecycle.classify(
                            job_id, source="lifecycle_workspace_list"
                        )
                        metadata["completion_lifecycle_disposition"] = (
                            decision.disposition
                        )
                        metadata["completion_lifecycle_route"] = decision.route
                        metadata["completion_lifecycle_command_id"] = (
                            decision.command_id
                        )
                        metadata["completion_lifecycle_deferred"] = decision.deferred
                        # Compatibility telemetry during the routed-veto
                        # migration. This is observational only; action-time
                        # ownership comes from the durable claim/router above.
                        metadata["completion_finalization_owned"] = (
                            decision.command_id is not None
                        )
                        metadata["completion_control_owned"] = (
                            decision.reason == "active_control_claim"
                        )
                    # Only a reapable-status parent can be torn down, so only
                    # then does the live-child guard matter — skip the query
                    # otherwise. Keys the guard on the real dependency (a critic
                    # SSHed into this pod), not on job_status alone.
                    if metadata["job_status"] in _REAPABLE_JOB_STATUSES:
                        metadata[
                            "has_live_shared_child"
                        ] = await self._live_shared_child_exists(
                            job_id, pod.metadata.name
                        )

            instances.append(
                Instance(
                    kind=self.kind,
                    id=pod.metadata.name,
                    version=labels.get("srw/build-sha"),
                    bound_to=thread_id or job_id,
                    metadata=metadata,
                )
            )
        return instances

    async def is_healthy(self, inst: Instance) -> bool:
        if _is_stateless_thread_instance(inst):
            # Only the terminal/loss protocol may classify and retire a
            # stateless session workspace.  In particular, a Failed pod must
            # not take the reconciler's immediate unhealthy-delete shortcut.
            return True
        if inst.metadata.get("completion_lifecycle_deferred"):
            # Live commands stand down; expired/parked commands have already
            # been nudged into the shared durable router.  Neither path may use
            # the generic unhealthy-delete shortcut.
            return True
        # Phase 2a: phase Running is the cheap signal. Phase 2b adds a
        # crash detector that catches Unknown/Failed pods explicitly.
        return inst.metadata.get("pod_phase") in (None, "Running", "Pending")

    async def is_idle(self, inst: Instance) -> bool:
        """A workspace is drainable when its bound work is paused/ended.

        We don't gate on ``last_activity > IDLE_TIMEOUT`` here — that's
        the existing ``workspace_idle_sweeper``'s domain (suspension on
        no-traffic). For drift detection we want to react as soon as the
        bound work is in a quiescent state, regardless of how long.
        """
        if _is_stateless_thread_instance(inst):
            return False
        if inst.metadata.get("completion_lifecycle_deferred"):
            return False
        if inst.metadata.get("has_live_shared_child"):
            return False
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        if job_status:
            # 'paused' gets a warm grace: it is often a human-wait (sudo/VM
            # approval, 24h TTL) and acting on the next tick destroys the
            # workspace minutes into that window.
            if paused_within_grace(inst.metadata):
                return False
            return job_status in _IDLE_JOB_STATUSES
        if thread_status:
            return thread_status in _IDLE_THREAD_STATUSES
        # No bound row → not safe to claim idle (pod may have been
        # created but DB context not yet persisted).
        return False

    async def is_reapable(self, inst: Instance) -> bool:
        """True when the bound work no longer needs the pod.

        Superset of ``is_idle``: adds terminal job/thread states. Terminal
        instances get cleaned up; suspendable-idle ones get snapshot+freed.

        A pod whose bound row is confirmed gone (``bound_row_missing`` — the
        job/thread was deleted) is an orphan and reapable once the pod is
        older than the orphan grace period. The age gate protects the
        pod-created-but-row-not-yet-persisted provisioning window, which is
        seconds long — the grace default is minutes. A pod whose row state is
        merely *unknown* (lookup failed) is never reapable.
        See knowledge-history/done/deleted_job_orphans_workspace_pod.md.

        Guard: a workspace shared by a live child job (a critic SSHed into the
        parent's pod) is never reapable, regardless of the parent's own status —
        reaping would strand the child. See ``_live_shared_child_exists`` and
        knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
        """
        if _is_stateless_thread_instance(inst):
            return False
        if inst.metadata.get("completion_lifecycle_deferred"):
            return False
        if inst.metadata.get("has_live_shared_child"):
            return False
        if inst.metadata.get("bound_row_missing"):
            age = inst.metadata.get("pod_age_s")
            return age is not None and age >= self._orphan_grace_s()
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        reapable = False
        if job_status:
            # Warm grace for 'paused' — see is_idle.
            if paused_within_grace(inst.metadata):
                return False
            reapable = job_status in _REAPABLE_JOB_STATUSES
        elif thread_status:
            reapable = thread_status in _REAPABLE_THREAD_STATUSES
        if not reapable:
            return False
        # A dirty Kubernetes runtime cannot become reapable without a durable,
        # renewable capture authority. Otherwise repeated refused snapshots
        # eventually hit give_up() and turn safe containment into delayed loss.
        if inst.metadata.get("pod_ip") and await self.is_dirty(inst):
            return False
        return True

    def _is_terminal(self, inst: Instance) -> bool:
        """Bound work is finished (vs merely paused) — nothing to preserve
        beyond an existing snapshot. A deleted row is definitively finished,
        so orphans count as terminal (give_up() won't recreate the pod).

        POD-scoped only. Whether the PVC may be DESTROYED is a strictly
        narrower question — see ``_is_volume_reclaimable``; an 'ended' thread is
        terminal here (its pod may go) and NOT reclaimable there (its volume
        must stay, because the session is resumable)."""
        if inst.metadata.get("bound_row_missing"):
            return True
        job_status = inst.metadata.get("job_status")
        thread_status = inst.metadata.get("thread_status")
        if job_status:
            return job_status in _TERMINAL_JOB_STATUSES
        if thread_status:
            return thread_status in _TERMINAL_THREAD_STATUSES
        return False

    def _is_volume_reclaimable(self, inst: Instance) -> bool:
        """May we DESTROY this instance's PVC? Strictly narrower than terminal.

        Pod teardown and volume reclaim are different decisions and this module
        must not conflate them:

        * **Jobs** — unchanged from the emptyDir era: a completed/failed/
          cancelled job is finished forever, so its volume goes with it. This is
          the "PVC dies when the job dies" guard that made Branch (a)
          acceptable after the orphan-PV leak class of the pre-emptyDir design.
        * **Sessions** — NO thread status reclaims. 'ended' is not death: the
          agent's idle-archive handler ends a session after 30 idle minutes and
          ``resume_thread`` requires precisely that status to bring it back. If
          'ended' reclaimed the volume, an ordinary coffee break would delete
          the user's working tree — worse than the emptyDir behavior PVCs
          replace. A session's volume is reclaimed only when the thread itself
          is gone. See knowledge-base/knowledge/features/workspace_pvc_backed_migration.md:186-191.

        ``bound_row_missing`` is the deletion signal for both kinds and the ONLY
        one for sessions: thread deletion is a hard ``DELETE FROM threads``
        (postgres.py ``delete_thread``), there is no 'deleted' status to read,
        and ``list_instances`` sets this flag only when the lookup SUCCEEDED and
        found no row. A lookup that merely failed leaves the metadata bare and
        lands on the ``False`` fall-through below — unknown state never
        destroys data.
        """
        # Row confirmed absent → the job/thread was permanently deleted. Nothing
        # can ever restore or resume from this volume, so it is pure garbage.
        # (The reconciler additionally age-gates orphans via ``is_reapable`` so
        # the provisioning window — pod created, row not yet persisted — cannot
        # be mistaken for a deletion.)
        if inst.metadata.get("bound_row_missing"):
            return True
        job_status = inst.metadata.get("job_status")
        if job_status:
            return job_status in _TERMINAL_JOB_STATUSES
        # Sessions (``thread_status`` present) and unknown/unbound instances both
        # fall through: keep the volume. Fail-safe wins — a leaked 10Gi volume is
        # an operational annoyance the backstop sweep collects once the thread is
        # actually deleted; a deleted one is unrecoverable user data.
        return False

    async def is_dirty(self, inst: Instance) -> bool:
        """True when the workspace may hold un-snapshotted state worth saving.

        Threads: precise — current ``total_turns`` vs the turn count recorded
        at last snapshot (``last_snapshot_turns``). Zero turns, or turns equal
        to the snapshot, means clean.

        Jobs: no monotonic turn counter exists in the app DB (audit count is in
        the audit store — deliberately not consulted here). Conservative: a terminal job
        with an existing snapshot is clean (it got a completion capture);
        otherwise dirty (attempt a snapshot; the escape hatch bounds the
        unreachable case).

        NOTE: never reads ``last_activity`` — it is bumped by the orchestrator's
        own context merges and cannot distinguish real work from bookkeeping.
        """
        # Orphan (bound row deleted): nothing is worth saving — no entity can
        # ever restore the snapshot, and record_attempt would merge into a
        # deleted row (silent no-op), retrying forever. Clean → direct delete.
        if inst.metadata.get("bound_row_missing"):
            return False
        thread_status = inst.metadata.get("thread_status")
        if thread_status is not None:
            turns = inst.metadata.get("total_turns") or 0
            snap_turns = inst.metadata.get("last_snapshot_turns")
            if snap_turns is None:
                return turns > 0
            return turns > snap_turns
        # Job path: no turn counter.
        if self._is_terminal(inst):
            return inst.metadata.get("snapshot_status") != "available"
        return True

    async def is_state_ephemeral(self, inst: Instance) -> bool:
        """True when pod-local storage dies with the pod (emptyDir).

        Ephemeral → a crashed/unreachable pod's state is unrecoverable, so the
        terminal action is delete-the-tombstone. PVC-backed → state survives on
        the volume; the terminal action is recreate-pod-keep-PVC. Defaults to
        ephemeral (today's fleet default) when the volume mode is unknown.
        """
        return bool(inst.metadata.get("volume_ephemeral", True))

    async def _tcp_probe(self, host: str, port: int) -> bool:
        """One-shot TCP connect with a short timeout. Overridable in tests."""

        def _connect() -> bool:
            try:
                with socket.create_connection((host, port), timeout=5):
                    return True
            except OSError:
                return False

        return await asyncio.to_thread(_connect)

    async def is_reachable(self, inst: Instance) -> bool:
        """Cached liveness probe to the pod's SSH port (30022).

        Used ONLY in the reap path to choose snapshot-vs-retry — never in
        ``is_healthy`` (an unreachable busy pod must not be force-deleted over
        a transient blip). Cached ~30s per pod IP.
        """
        host = inst.metadata.get("pod_ip")
        if not host:
            return False
        now = self._clock()
        cached = self._reach_cache.get(host)
        if cached is not None and (now - cached[0]) < self._reach_ttl_s:
            return cached[1]
        ok = await self._tcp_probe(host, 30022)
        self._reach_cache[host] = (now, ok)
        return ok

    def _max_attempts(self) -> int:
        try:
            return int(os.environ.get("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5"))
        except ValueError:
            return 5

    def _orphan_grace_s(self) -> float:
        return orphan_grace_seconds()

    async def attempts_exhausted(self, inst: Instance) -> bool:
        return (inst.metadata.get("snapshot_attempts") or 0) >= self._max_attempts()

    @staticmethod
    def _legacy_permit(inst: Instance) -> LifecycleActionPermit:
        return LifecycleActionPermit(
            LifecycleRouteDecision(
                job_id=str(inst.bound_to or inst.id),
                disposition="legacy",
            )
        )

    @asynccontextmanager
    async def lifecycle_action(
        self,
        inst: Instance,
        *,
        source: str,
    ) -> AsyncIterator[LifecycleActionPermit]:
        """Hold one command/control-visible claim across snapshot -> delete."""

        existing = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(existing, LifecycleActionPermit):
            yield existing
            return

        labels = inst.metadata.get("labels") or {}
        job_bound = (
            "srw/thread-id" not in labels
            and inst.metadata.get("thread_status") is None
            and bool(inst.bound_to)
        )
        if self._completion_lifecycle is None or not job_bound:
            permit = self._legacy_permit(inst)
            if await self._kubernetes_snapshot_capture_contained(inst, source=source):
                permit.skip("k8s_capture_authority_unavailable")
            inst.metadata["_completion_lifecycle_permit"] = permit
            try:
                yield permit
            finally:
                inst.metadata.pop("_completion_lifecycle_permit", None)
            return

        expected_status = str(
            inst.metadata.get("job_status")
            or ("missing" if inst.metadata.get("bound_row_missing") else "unknown")
        )
        expected_lane = str(inst.metadata.get("execution_lane") or "pinned")
        async with self._completion_lifecycle.action(
            str(inst.bound_to),
            source=f"lifecycle_workspace_{source}"[:64],
            resource_kind="workspace",
            resource_identity=str(inst.metadata.get("pod_uid") or inst.id),
            expected_status=expected_status,
            expected_lane=expected_lane,
        ) as permit:
            inst.metadata["_completion_lifecycle_permit"] = permit
            try:
                if permit.local and self._provisioner_ready():
                    if await self._kubernetes_snapshot_capture_contained(
                        inst, source=source
                    ):
                        permit.skip("k8s_capture_authority_unavailable")
                        yield permit
                        return
                    if not await self._permit_external(permit):
                        yield permit
                        return
                    owner = WorkspaceOwner.job(str(inst.bound_to))
                    needs_snapshot_authority = bool(
                        source in {"reap", "snapshot"}
                        and inst.metadata.get("pod_ip")
                        and self._snapshot is not None
                        and getattr(self._snapshot, "is_available", False)
                        and await self.is_dirty(inst)
                        and await self.is_reachable(inst)
                    )
                    listed_uid = inst.metadata.get("pod_uid")
                    listed_pod_ip = inst.metadata.get("pod_ip")
                    identity: WorkspaceTeardownIdentity | None = None
                    # Identity capture and attestation are read-only probes.
                    # Their failure (e.g. a ws_client handshake error) proves
                    # no teardown I/O ran under this claim, so it settles the
                    # claim: retaining it would fence the job's dispatch and
                    # controls for the whole lifecycle lease over a failed
                    # read (lifecycle_reap_failure_parks_running_stateless_job).
                    # The next tick retries; nothing is torn down meanwhile.
                    probe_error: Exception | None = None
                    if source == "orphan_pvc":
                        # Detached-resource GC has no live runtime from which
                        # to prepare process zero. Keep exact resource capture
                        # under the completion claim; runtime teardown itself
                        # prepares durable cleanup before resource observation.
                        try:
                            async with asyncio.timeout(
                                LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS
                            ):
                                identity = await self._provisioner.capture_workspace_teardown_identity(
                                    owner
                                )
                        except Exception as exc:
                            probe_error = exc
                    elif needs_snapshot_authority:
                        if not await self._permit_external(permit):
                            yield permit
                            return
                        try:
                            async with asyncio.timeout(
                                LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS
                            ):
                                attestation = (
                                    await self._provisioner.attest_workspace_runtime(
                                        owner
                                    )
                                )
                        except Exception as exc:
                            probe_error = exc
                    if probe_error is not None:
                        logger.warning(
                            "Workspace identity probe failed for %s (source=%s); "
                            "releasing the lifecycle claim without teardown",
                            inst.id,
                            source,
                            exc_info=probe_error,
                        )
                        permit.skip("workspace_identity_probe_failed", settled=True)
                        yield permit
                        return
                    if needs_snapshot_authority:
                        if (
                            listed_uid is None
                            or attestation.runtime_incarnation != str(listed_uid)
                            or (
                                listed_pod_ip is not None
                                and attestation.pod_ip != str(listed_pod_ip)
                            )
                        ):
                            permit.skip("workspace_identity_changed", settled=True)
                            yield permit
                            return
                        identity = WorkspaceTeardownIdentity(
                            pod_uid=attestation.runtime_incarnation,
                            pvc_uid=None,
                            service_uid=None,
                            pod_ip=attestation.pod_ip,
                            ssh_host_key_fingerprint=(
                                attestation.ssh_host_key_fingerprint
                            ),
                            ssh_port=attestation.port,
                        )
                    if identity is not None and (
                        (listed_uid is not None and identity.pod_uid != str(listed_uid))
                        or (
                            listed_pod_ip is not None
                            and identity.pod_ip is not None
                            and identity.pod_ip != str(listed_pod_ip)
                        )
                    ):
                        # The object listed by this tick is already gone.  A
                        # same-name replacement is not lifecycle's target.
                        permit.skip("workspace_identity_changed", settled=True)
                    elif (
                        identity is not None
                        and identity.pod_uid is None
                        and source != "orphan_pvc"
                        and not inst.metadata.get("bound_row_missing")
                    ):
                        permit.skip("workspace_identity_absent", settled=True)
                    elif (
                        source == "orphan_pvc"
                        and identity is not None
                        and identity.pvc_uid is None
                    ):
                        permit.skip("workspace_pvc_identity_unknown")
                    elif identity is not None:
                        inst.metadata["_completion_lifecycle_workspace_identity"] = (
                            identity
                        )
                yield permit
            finally:
                inst.metadata.pop("_completion_lifecycle_permit", None)
                inst.metadata.pop("_completion_lifecycle_workspace_identity", None)

    async def _kubernetes_snapshot_capture_contained(
        self, inst: Instance, *, source: str
    ) -> bool:
        """Whether this lifecycle action would require an unsafe Pod read."""

        return bool(
            source in {"reap", "snapshot"}
            and await self.is_reapable(inst)
            and inst.metadata.get("pod_ip")
            and self._snapshot is not None
            and getattr(self._snapshot, "is_available", False)
            and await self.is_dirty(inst)
        )

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

    async def record_attempt(self, inst: Instance) -> None:
        """Persist an incremented snapshot-attempt counter to the bound row."""
        if _is_stateless_thread_instance(inst):
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
        labels = inst.metadata.get("labels") or {}
        try:
            if "srw/thread-id" in labels:
                await self._db.merge_thread_workspace_context(
                    bound, {"snapshot_attempts": nxt}
                )
            else:
                await self._db.merge_workspace_container_context(
                    bound, {"snapshot_attempts": nxt}
                )
        except Exception:
            logger.exception("Failed to record snapshot attempt for %s", inst.id)
            return
        if isinstance(permit, LifecycleActionPermit):
            permit.complete()

    async def give_up(self, inst: Instance, grace_s: int) -> None:
        """Escape hatch: dirty + unreachable + attempts exhausted.

        Ephemeral storage → delete the pod (state already unrecoverable).
        PVC-backed + non-terminal → recreate the pod against the same PVC so the
        volume reattaches; the PVC is NOT deleted. PVC-backed + terminal → do
        NOT recreate: for a finished job the ``delete`` call below reclaimed the
        volume too, and for an 'ended' (idle) session the volume is deliberately
        kept but the pod is not wanted — the next ``/resume`` runs
        ``create_workspace``, which reattaches that same claim by its
        deterministic name. Either way a fresh pod here would be pointless (and,
        for the job, would race the reclaim). ``_is_terminal`` is therefore the
        right predicate for this branch, NOT ``_is_volume_reclaimable``: the
        question is "does anyone still need a running pod", not "may the volume
        die". Branch (a): see workspace_pvc_branch_a_implementation.md.
        """
        if self._completion_lifecycle is None:
            if _is_stateless_thread_instance(inst):
                return
            bound = inst.bound_to
            if not bound:
                return
            labels = inst.metadata.get("labels") or {}
            owner = (
                WorkspaceOwner.session(bound)
                if "srw/thread-id" in labels
                else WorkspaceOwner.job(bound)
            )
            await self.delete(inst, grace_s)
            if not self._is_terminal(inst) and not inst.metadata.get(
                "volume_ephemeral", True
            ):
                try:
                    if owner.kind == "session":
                        await self._provisioner.create_pinned_thread_workspace(owner.id)
                    else:
                        await self._provisioner.create_workspace(owner)
                except Exception:
                    logger.exception("PVC give_up recreate failed for %s", inst.id)
            return

        async with self._action_scope(inst, source="give_up") as (permit, _owns):
            if not permit.local or _is_stateless_thread_instance(inst):
                return
            bound = inst.bound_to
            if not bound:
                return
            labels = inst.metadata.get("labels") or {}
            owner = (
                WorkspaceOwner.session(bound)
                if "srw/thread-id" in labels
                else WorkspaceOwner.job(bound)
            )
            if not await self._permit_external(permit):
                return
            if not await self._delete_owned(inst, grace_s):
                return
            if not self._is_terminal(inst) and not inst.metadata.get(
                "volume_ephemeral", True
            ):
                if not await self._permit_external(permit):
                    return
                try:
                    async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                        if owner.kind == "session":
                            await self._provisioner.create_pinned_thread_workspace(
                                owner.id
                            )
                        else:
                            await self._provisioner.create_workspace(owner)
                    if not await self._permit_external(permit):
                        return
                except Exception:
                    logger.exception("PVC give_up recreate failed for %s", inst.id)
                    return
            permit.complete()

    async def snapshot(self, inst: Instance) -> str | None:
        """Capture the workspace contents to S3.

        Returns a snapshot reference token (currently the bound id —
        SnapshotService keys by job/thread id), or ``None`` if the
        snapshot path isn't usable for this instance.
        """
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
        """Snapshot with lifecycle/command ownership already decided."""

        if _is_stateless_thread_instance(inst):
            return None
        if self._snapshot is None or not getattr(self._snapshot, "is_available", False):
            return None
        if inst.metadata.get("pod_ip"):
            # A one-shot UID/key attestation cannot fence the unbounded
            # snapshot/S3 tail across crashes. Refuse before reachability, SSH,
            # S3, or attempt mutation until a durable renewable capture lease
            # covers this lifecycle path.
            logger.warning(
                "Lifecycle Kubernetes snapshot refused without durable "
                "capture authority for %s",
                inst.id,
            )
            return None
        ssh_host = inst.metadata.get("pod_ip")
        if not ssh_host:
            return None
        bound = inst.bound_to
        if not bound:
            return None
        permit = inst.metadata.get("_completion_lifecycle_permit")
        identity = inst.metadata.get("_completion_lifecycle_workspace_identity")
        if (
            self._completion_lifecycle is not None
            and isinstance(permit, LifecycleActionPermit)
            and permit.claim is not None
            and (
                not isinstance(identity, WorkspaceTeardownIdentity)
                or not identity.pod_ip
                or not identity.ssh_host_key_fingerprint
            )
        ):
            permit.skip("snapshot_identity_unattested", settled=True)
            return None
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return None
        try:
            async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                ok = await self._snapshot.capture_vm_snapshot(
                    job_id=bound,
                    ssh_host=(identity.pod_ip if identity is not None else ssh_host),
                    ssh_port=(identity.ssh_port if identity is not None else 30022),
                    source_type="pod",
                    entity_type=(
                        "threads"
                        if "srw/thread-id" in (inst.metadata.get("labels") or {})
                        else "jobs"
                    ),
                    work_marker=inst.metadata.get("total_turns"),
                    **(
                        {
                            "expected_host_key_fingerprint": (
                                identity.ssh_host_key_fingerprint
                            ),
                            "expected_runtime_incarnation": identity.pod_uid,
                        }
                        if identity is not None
                        and identity.ssh_host_key_fingerprint is not None
                        else {}
                    ),
                )
            if ok:
                # Mark the in-memory instance too — the reconciler calls
                # delete() right after a successful snapshot(), and delete()
                # reads this to decide the reap-and-restore handoff below.
                inst.metadata["snapshot_status"] = "available"
                # Success clears the escape-hatch retry counter.
                labels = inst.metadata.get("labels") or {}
                try:
                    if isinstance(
                        permit, LifecycleActionPermit
                    ) and not await self._permit_external(permit):
                        return None
                    if "srw/thread-id" in labels:
                        await self._db.merge_thread_workspace_context(
                            bound, {"snapshot_attempts": 0}
                        )
                    else:
                        await self._db.merge_workspace_container_context(
                            bound, {"snapshot_attempts": 0}
                        )
                except Exception:
                    logger.exception("Failed to reset attempts for %s", inst.id)
                if isinstance(
                    permit, LifecycleActionPermit
                ) and not await self._permit_external(permit):
                    return None
                return bound
            return None
        except Exception:
            logger.exception(
                "Snapshot failed for workspace %s (bound=%s)", inst.id, bound
            )
            return None

    async def restore(self, inst: Instance, snapshot_ref: str) -> None:
        """Restore the workspace from a snapshot reference.

        Phase 2a delegates to the existing suspension service so the
        provisioner-dispatch logic (K8s/Docker/VM) is shared. The
        ``snapshot_ref`` is the bound job/thread id — that's how the
        suspension service keys the restore.
        """
        if self._suspension is None or not getattr(
            self._suspension, "is_enabled", False
        ):
            return
        if "srw/thread-id" in inst.metadata.get("labels", {}):
            await self._suspension.restore_thread_workspace(snapshot_ref)
        else:
            await self._suspension.restore_workspace(snapshot_ref)

    async def signal_drain_pending(self, inst: Instance) -> None:
        """No-op: workspaces have no in-pod drain hook to react to a
        soft signal. The reconciler picks them up on a future tick
        when the bound job/thread becomes idle (paused/ended), and
        ``drain`` actuates immediately at that point."""
        return None

    async def drain(self, inst: Instance, grace_s: int) -> None:
        """Delete the pod (snapshot has already run when reached via tick).

        The reconciler tick calls ``snapshot()`` before ``drain()`` for
        stateful kinds, so this method is the pure delete step. Direct
        delete calls (via the orchestrator's ad-hoc paths) still go
        through the existing services and don't pass through here.
        """
        if _is_stateless_thread_instance(inst):
            return
        await self.delete(inst, grace_s)

    async def delete(self, inst: Instance, grace_s: int) -> None:
        if self._completion_lifecycle is None:
            await self._delete_owned(inst, grace_s)
            return
        async with self._action_scope(inst, source="delete") as (permit, _owns):
            if not permit.local:
                return
            if await self._delete_owned(inst, grace_s):
                permit.complete()

    async def _delete_owned(self, inst: Instance, grace_s: int) -> bool:
        """Delete only the identity captured inside the lifecycle claim."""

        if _is_stateless_thread_instance(inst):
            return False
        if not self._provisioner_ready():
            return False
        bound = inst.bound_to
        if not bound:
            logger.debug("delete skipped: no bound job/thread for %s", inst.id)
            return False
        labels = inst.metadata.get("labels") or {}
        owner = (
            WorkspaceOwner.session(bound)
            if "srw/thread-id" in labels
            else WorkspaceOwner.job(bound)
        )
        permit = inst.metadata.get("_completion_lifecycle_permit")
        if isinstance(
            permit, LifecycleActionPermit
        ) and not await self._permit_external(permit):
            return False

        listed_runtime = inst.metadata.get("pod_uid")
        if listed_runtime is None:
            logger.warning(
                "Workspace lifecycle delete lacks an immutable runtime UID for %s",
                inst.id,
            )
            return False

        runtime_incarnation = str(listed_runtime)
        target_disposition = (
            "suspended"
            if (
                not self._is_terminal(inst)
                and inst.metadata.get("volume_ephemeral", True)
                and inst.metadata.get("snapshot_status") == "available"
            )
            else "deleted"
        )
        reclaim_shared_resources = self._is_volume_reclaimable(inst)
        try:
            intent = await self._provisioner.prepare_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=runtime_incarnation,
                target_disposition=target_disposition,
                reclaim_shared_resources=reclaim_shared_resources,
                suspended_at=(
                    datetime.now(timezone.utc).isoformat()
                    if target_disposition == "suspended"
                    else None
                ),
                snapshot_restore_required=(target_disposition == "suspended"),
                allow_orphan=bool(inst.metadata.get("bound_row_missing")),
                admission_source="automatic",
            )
            if not isinstance(intent, dict):
                return False
            if isinstance(
                permit, LifecycleActionPermit
            ) and not await self._permit_external(permit):
                return False
            async with asyncio.timeout(LIFECYCLE_EXTERNAL_TIMEOUT_SECONDS):
                cleanup = await self._provisioner.reconcile_workspace_cleanup_intent(
                    owner,
                    expected_runtime_incarnation=runtime_incarnation,
                    intent_generation=int(intent["intent_generation"]),
                )
            if isinstance(
                permit, LifecycleActionPermit
            ) and not await self._permit_external(permit):
                return False
        except Exception:
            logger.exception("Failed to reconcile workspace cleanup for %s", inst.id)
            return False
        if not isinstance(cleanup, WorkspaceCleanupOutcome):
            logger.error("Workspace lifecycle cleanup returned an untyped outcome")
            return False
        if cleanup.superseded:
            # The exact predecessor reached process-zero. Its successor owns
            # every current row and deterministic Kubernetes resource.
            return True
        if not cleanup.settled:
            return False
        if target_disposition == "suspended":
            logger.info(
                "Reaped workspace %s settled as suspended from its durable snapshot",
                inst.id,
            )
        return True

    async def _permit_external(self, permit: LifecycleActionPermit) -> bool:
        if not permit.local:
            return False
        if self._completion_lifecycle is None or permit.claim is None:
            return True
        return await self._completion_lifecycle.refresh(permit)

    async def _reconcile_terminal_job_workspace_resources(
        self,
        job_id: str,
        row: Any,
        *,
        permit: LifecycleActionPermit | None = None,
    ) -> bool:
        """Reclaim a terminal job's exact PVC/Service through S36 only."""

        raw_context = row.get("context") if row is not None else None
        if isinstance(raw_context, str):
            try:
                raw_context = json.loads(raw_context)
            except (TypeError, ValueError):
                return False
        workspace = (
            raw_context.get("workspace_container")
            if isinstance(raw_context, dict)
            else None
        )
        runtime = (
            workspace.get("_runtime_incarnation")
            if isinstance(workspace, dict) and workspace.get("provisioner") == "k8s"
            else None
        )
        if not isinstance(runtime, str):
            return False
        try:
            from uuid import UUID

            if str(UUID(runtime)) != runtime:
                return False
        except (TypeError, ValueError):
            return False

        owner = WorkspaceOwner.job(job_id)
        try:
            intent = await self._provisioner.prepare_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=runtime,
                target_disposition="deleted",
                reclaim_shared_resources=True,
                admission_source="automatic",
            )
            if (
                not isinstance(intent, dict)
                or intent.get("resources_captured_at") is None
                or intent.get("reclaim_shared_resources") is not True
            ):
                return False
            if permit is not None and not await self._permit_external(permit):
                return False
            cleanup = await self._provisioner.reconcile_workspace_cleanup_intent(
                owner,
                expected_runtime_incarnation=runtime,
                intent_generation=int(intent["intent_generation"]),
            )
            if permit is not None and not await self._permit_external(permit):
                return False
        except Exception:
            # A committed intent or settlement survives this process. The next
            # sweep must replay it; never fall back to raw Kubernetes deletion.
            logger.exception(
                "Terminal workspace cleanup reconciliation failed for job %s",
                job_id,
            )
            return False
        return isinstance(cleanup, WorkspaceCleanupOutcome) and cleanup.settled

    async def reap_orphans(self) -> int:
        """Backstop reconciliation for already-authorized workspace cleanup.

        The inline delete (``delete()``) handles the common path, but a
        PVC can outlive its pod — the pod was already gone when teardown ran, the
        inline delete failed, or the orchestrator restarted mid-teardown. Such a
        PVC never surfaces as a live ``Instance`` (it has no pod), so the
        reconciler's per-instance reap can't see it. This once-per-tick sweep
        lists workspace PVCs directly.  Owner absence is inventory evidence,
        not process-zero authority: a lost create response can leave a real Pod,
        Service, or PVC after its database owner disappears.  Consequently:

        * **Job** PVCs (``pvc-workspace-*``, ``srw/job-id``) — reconciled only
          for a still-present terminal row through its exact durable cleanup
          intent and process-zero receipt.  Missing owners are retained.
        * **Session** PVCs (``pvc-ws-thread-*`` for the workspace pod,
          ``pvc-agent-s-*`` for the session agent pod, both ``srw/thread-id``) —
          never reaped merely because the ``threads`` row is absent.  Status is
          likewise insufficient: an 'ended' thread is resumable.  Only the
          supported retirement path can create the exact terminal-reclaim
          authority; otherwise these claims remain visible for inventory and
          operator reconciliation.

        Everything else is left alone: the shared ``srw-workspace`` agent-scratch
        claim, unlabeled claims, foreign names. emptyDir fleets have no such PVCs
        → no-op. Runs regardless of ``WORKSPACE_PVC_ENABLED`` so a rollback (flag
        flipped off) still drains leftover PVCs as their work finishes.

        Every uncertain or authority-free branch keeps the volume: DB error,
        unreadable owner id, live pod, missing owner, or unadmitted cleanup all
        ``continue`` without deleting.

        Returns the number of PVCs deleted.
        """
        if not self._provisioner_ready() or self._db is None:
            return 0
        reaped = 0
        reconcile_pending = getattr(
            type(self._provisioner),
            "reconcile_pending_workspace_cleanup_intents",
            None,
        )
        if callable(reconcile_pending):
            try:
                pending_counts = await reconcile_pending(self._provisioner, limit=25)
                if isinstance(pending_counts, dict):
                    reaped += int(pending_counts.get("settled") or 0)
                    reaped += int(pending_counts.get("superseded") or 0)
            except Exception:
                logger.exception("Pending workspace cleanup reconciliation failed")
        core = self._provisioner._core_api
        ns = self._provisioner._namespace
        try:
            pvcs = await asyncio.to_thread(
                core.list_namespaced_persistent_volume_claim,
                namespace=ns,
                label_selector=self._label_selector,
            )
        except Exception:
            logger.exception("Orphan PVC sweep: list failed")
            return reaped
        items = list(getattr(pvcs, "items", []) or [])
        if not items:
            return reaped
        # One pod list → the owner ids that still have a live workspace pod.
        # Never reap a PVC out from under a running pod; the instance path tears
        # down pod+PVC together for those. Thread ids are also stored truncated
        # to the PVC-name length so a name-derived reference (see below) matches
        # a live pod whose label carries the full uuid.
        #
        # Note the selector only covers *workspace* pods; a session's agent pod
        # (``srw-agent-s-*``, own claim ``pvc-agent-s-*``) is not in it. That is
        # tolerable because the reclaim gate for sessions is "the thread row is
        # gone" — at which point the agent pod is being torn down too — and K8s'
        # pvc-protection finalizer holds a still-mounted claim in Terminating
        # rather than yanking the volume out from under a running pod.
        live_job_ids: set[str] = set()
        live_thread_ids: set[str] = set()
        for pod in await self._list_pods():
            pod_labels = pod.metadata.labels or {}
            pod_job_id = pod_labels.get("srw/job-id")
            if pod_job_id:
                live_job_ids.add(pod_job_id)
            pod_thread_id = pod_labels.get("srw/thread-id")
            if pod_thread_id:
                live_thread_ids.add(pod_thread_id)
                live_thread_ids.add(pod_thread_id[:_PVC_ID_PREFIX_LEN])
        for pvc in items:
            name = pvc.metadata.name
            pvc_labels = pvc.metadata.labels or {}
            job_id = pvc_labels.get("srw/job-id")
            session_prefix = next(
                (p for p in _SESSION_PVC_PREFIXES if name.startswith(p)), None
            )
            if session_prefix:
                # Owner reference: the full thread uuid from the label (both
                # provisioners stamp it) with the name's 12-char id suffix as a
                # fallback, so a label-less legacy claim remains inventory-
                # visible instead of being silently classified as foreign.
                # Strip by prefix length, never by
                # splitting on '-' — a truncated uuid contains one.
                thread_ref = (
                    pvc_labels.get("srw/thread-id") or name[len(session_prefix) :]
                )
                if not thread_ref:
                    continue
                if (
                    thread_ref in live_thread_ids
                    or thread_ref[:_PVC_ID_PREFIX_LEN] in live_thread_ids
                ):
                    continue
                exists = await self._thread_row_exists(thread_ref)
                # None = lookup failed (unknown) and True = thread still there
                # (ended sessions live here — resumable, volume stays). A
                # definitive False still lacks the exact retired runtime and
                # process-zero receipt required by S36, so retain the legacy
                # PVC/Service for explicit inventory rather than guessing.
                if exists is not False:
                    continue
                logger.warning(
                    "Retaining ownerless session workspace resources without "
                    "exact runtime/process-zero authority (thread_ref=%s pvc=%s)",
                    thread_ref,
                    name,
                )
                continue
            # Job workspace PVCs (pvc-workspace-*). Anything else — the shared
            # agent-scratch PVC, unlabeled claims, foreign names — is out of
            # scope and never touched.
            if not job_id or not name.startswith(_JOB_PVC_PREFIX):
                continue
            if job_id in live_job_ids:
                continue
            # Resolve existence/status with a DIRECT query that separates the
            # three cases — a transient DB error must NOT look like "job gone"
            # and trigger a wrong delete. row present → use status; no row →
            # ownerless residue (retain); query raised → unknown (retain).
            try:
                async with self._db.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT status, execution_lane, context FROM jobs "
                        "WHERE id = $1::uuid",
                        job_id,
                    )
            except Exception:
                logger.exception(
                    "Orphan PVC sweep: job lookup failed for %s — skipping", job_id
                )
                continue
            status = row["status"] if row else None
            if row is not None and status not in _TERMINAL_JOB_STATUSES:
                continue  # job still active → keep its volume
            if row is None:
                logger.warning(
                    "Retaining ownerless job workspace resources without exact "
                    "runtime/process-zero authority (job=%s pvc=%s)",
                    job_id,
                    name,
                )
                continue
            reconciled = False
            if self._completion_lifecycle is None:
                reconciled = await self._reconcile_terminal_job_workspace_resources(
                    job_id, row
                )
            else:
                workspace = row.get("context") or {}
                if isinstance(workspace, str):
                    try:
                        workspace = json.loads(workspace)
                    except (TypeError, ValueError):
                        workspace = {}
                workspace = (
                    workspace.get("workspace_container")
                    if isinstance(workspace, dict)
                    else None
                )
                orphan_inst = Instance(
                    kind=self.kind,
                    id=name,
                    bound_to=job_id,
                    metadata={
                        "labels": {"srw/job-id": job_id},
                        "job_status": str(status),
                        "execution_lane": str(row.get("execution_lane") or "pinned"),
                        "pod_uid": (
                            workspace.get("_runtime_incarnation")
                            if isinstance(workspace, dict)
                            else None
                        ),
                    },
                )
                async with self.lifecycle_action(
                    orphan_inst, source="orphan_pvc"
                ) as permit:
                    if permit.local and await self._permit_external(permit):
                        reconciled = (
                            await self._reconcile_terminal_job_workspace_resources(
                                job_id, row, permit=permit
                            )
                        )
                        if reconciled:
                            permit.complete()
            if reconciled:
                reaped += 1
                logger.info(
                    "Terminal workspace resources reconciled: %s (job=%s status=%s)",
                    name,
                    job_id,
                    status,
                )
        if reaped:
            logger.warning(
                "Workspace cleanup reconciler settled %d exact durable "
                "cleanup intent(s); ownerless resources were retained",
                reaped,
            )
        return reaped

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _provisioner_ready(self) -> bool:
        return bool(getattr(self._provisioner, "_k8s_available", False))

    async def _list_pods(self) -> list[Any]:
        try:
            result = await asyncio.to_thread(
                self._provisioner._core_api.list_namespaced_pod,
                namespace=self._provisioner._namespace,
                label_selector=self._label_selector,
            )
        except Exception:
            logger.exception("Failed to list workspace pods for lifecycle")
            return []
        return list(result.items)

    async def _fetch_job(self, job_id: str) -> dict[str, Any] | None | Any:
        """Bound-row lookup: the row, None when confirmed absent, or
        ``_FETCH_FAILED`` when the lookup itself failed (DB error / no DB)."""
        if self._db is None:
            return _FETCH_FAILED
        try:
            return await self._db.get_job(job_id)
        except Exception:
            logger.exception("Failed to fetch job %s for workspace lifecycle", job_id)
            return _FETCH_FAILED

    async def _fetch_thread(self, thread_id: str) -> dict[str, Any] | None | Any:
        """Same three-way contract as ``_fetch_job``."""
        if self._db is None:
            return _FETCH_FAILED
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT id, status, ended_at, agent_id, execution_lane, "
                    "total_turns, metadata "
                    "FROM threads WHERE id = $1",
                    thread_id,
                )
            return dict(row) if row else None
        except Exception:
            logger.exception(
                "Failed to fetch thread %s for workspace lifecycle", thread_id
            )
            return _FETCH_FAILED

    async def _thread_row_exists(self, thread_ref: str) -> bool | None:
        """Does a ``threads`` row still exist for a session PVC's owner?

        Three-way, like ``_fetch_job``/``_fetch_thread``: ``True`` (row present),
        ``False`` (lookup succeeded and found nothing), ``None`` (the lookup
        itself failed / no DB).  Neither ``False`` nor ``None`` authorizes a
        volume mutation; both are used only to classify retained inventory.

        This is an inventory signal for sessions, not deletion authority:
        deleting a thread is a hard ``DELETE FROM threads``
        (``postgres.delete_thread``) — there is no 'deleted' status, and the
        statuses that DO exist ('ended', 'suspended') are all resumable.

        ``thread_ref`` is normally the full uuid from the PVC's ``srw/thread-id``
        label, but falls back to the 12-char id embedded in the PVC name, so the
        comparison is a left-prefix of the reference's own length rather than an
        equality on ``id``. A prefix could in principle match a *different*
        thread; the only consequence is "row found" versus "ownerless" in the
        safe retained inventory. Lower-casing matches ``uuid::text`` output.
        """
        if self._db is None:
            return None
        ref = (thread_ref or "").strip().lower()
        if not ref:
            return None
        try:
            async with self._db.acquire() as conn:
                row = await conn.fetchrow(
                    # Casts are explicit so the parameter's type never has to be
                    # inferred from an overloaded function (``length`` accepts
                    # text and bytea) — an ambiguous Parse here would surface as
                    # a lookup failure, i.e. a volume we then keep forever.
                    "SELECT id FROM threads "
                    "WHERE left(id::text, length($1::text)) = $1::text LIMIT 1",
                    ref,
                )
        except Exception:
            logger.exception(
                "Orphan PVC sweep: thread lookup failed for %s — skipping", ref
            )
            return None
        return row is not None

    async def _live_shared_child_exists(
        self, parent_job_id: str, pod_name: str | None
    ) -> bool:
        """True if a non-terminal child job shares this parent's workspace pod.

        A critic verification subjob inherits the parent's ``workspace_container``
        at spawn (``_trigger_verification_on_complete`` copies it) and SSHes into
        the *parent's* pod instead of getting its own. While such a child is
        alive, reaping the parent pod strands it (headless Service with zero
        endpoints → NXDOMAIN), which is the P0 bug. Match on the inherited
        ``pod_name`` — stable (unlike ``pod_ip``, which churns on restore) and
        exact: the critic's copy equals this pod's name, whereas a delegation
        child (which gets its own pod) does not, so the guard stays narrow.

        Fail-safe: on a DB error, assume a child is present (do not reap) —
        mirrors ``reap_orphans``' "DB error → don't delete" stance.
        """
        if self._db is None or not pod_name:
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
                              AND context->'workspace_container'->>'pod_name' = $2
                        )
                        """,
                        parent_job_id,
                        pod_name,
                    )
                )
        except Exception:
            logger.exception(
                "Failed to check live shared child for pod %s (parent %s); "
                "assuming shared — not reaping",
                pod_name,
                parent_job_id,
            )
            return True
