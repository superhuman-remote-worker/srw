"""Owner-keyed workspace lifecycle: one provisioning path for jobs and sessions.

See knowledge-history/done/unified_workspace_provisioning.md.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, Optional

OwnerKind = Literal["job", "session"]


@dataclass(frozen=True)
class WorkspaceOwner:
    """Identifies whose workspace this is. Collapses the job/thread split."""

    kind: OwnerKind
    id: str

    @classmethod
    def job(cls, job_id: str) -> "WorkspaceOwner":
        return cls("job", job_id)

    @classmethod
    def session(cls, thread_id: str) -> "WorkspaceOwner":
        return cls("session", thread_id)

    @property
    def pod_name(self) -> str:
        prefix = "workspace" if self.kind == "job" else "ws-thread"
        return f"{prefix}-{self.id[:12]}"

    @property
    def label_key(self) -> str:
        return "srw/job-id" if self.kind == "job" else "srw/thread-id"

    @property
    def component_label(self) -> str:
        return "workspace" if self.kind == "job" else "thread-workspace"

    @property
    def network_tier_kind(self) -> str:
        # Arg expected by ContainerProvisioner._resolve_network_tier / DB.
        return "job" if self.kind == "job" else "thread"


class EnsureOutcome(Enum):
    READY = "ready"  # workspace usable now → caller may dispatch
    PENDING = "pending"  # in progress (creating/restoring/created) → caller retries next cycle
    FAILED = "failed"  # creation failed / terminal → caller decides policy


_RESTORE_REQUIRED_STATUSES = {
    None,
    "",
    "none",
    "deleted",
    "failed",
    "suspended",
    "restoring",
    "created",
    "creating",
    "pending",
    "ready",
}


@dataclass
class EnsureResult:
    outcome: EnsureOutcome
    status: Optional[str] = None


async def _create(
    owner: "WorkspaceOwner",
    provisioner,
    *,
    stateless_creation_generation: str | None = None,
    allow_stateless_create: bool = False,
    pinned_runtime_lock_held: bool = False,
) -> "EnsureResult":
    strict_kwargs: dict[str, Any] = {}
    if stateless_creation_generation is not None:
        strict_kwargs = {
            "stateless_creation_generation": stateless_creation_generation,
            "allow_stateless_create": allow_stateless_create,
        }
    if owner.kind == "session" and stateless_creation_generation is None:
        pinned_create = getattr(provisioner, "create_pinned_thread_workspace", None)
        if not callable(pinned_create):
            ok = False
        else:
            pinned_kwargs: dict[str, Any] = {}
            if pinned_runtime_lock_held:
                pinned_kwargs["runtime_lock_held"] = True
            ok = await pinned_create(owner.id, **pinned_kwargs)
    else:
        ok = await provisioner.create_workspace(
            owner,
            **strict_kwargs,
        )
    return EnsureResult(
        EnsureOutcome.PENDING if ok else EnsureOutcome.FAILED,
        status="creating" if ok else "failed",
    )


async def _ensure_existing_runtime(
    owner: "WorkspaceOwner",
    *,
    provisioner,
    current_status: Optional[str],
    expected_runtime_incarnation: str,
    stateless_creation_generation: str | None,
) -> "EnsureResult":
    """Reconcile one cached physical runtime without crossing its UID fence.

    Kubernetes object absence is not proof that the cached Pod's processes are
    gone: an unreachable/partitioned kubelet may still be running them.  A
    same-name replacement is likewise not authority over the cached UID.  Only
    observing that exact UID with every container terminated permits deletion
    and recreation here.
    """

    authority_probe = getattr(provisioner, "workspace_pod_authority", None)
    if not callable(authority_probe):
        return EnsureResult(EnsureOutcome.PENDING, status=current_status)
    try:
        authority = await authority_probe(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
    except Exception:
        # A control-plane failure is indistinguishable from a partitioned old
        # runtime at this layer.  Retrying without effects is the safe action.
        return EnsureResult(EnsureOutcome.PENDING, status=current_status)

    if authority == "exact_live":
        if current_status == "ready":
            return EnsureResult(EnsureOutcome.READY, status="ready")
        continuation = getattr(
            provisioner, "continue_stateless_workspace_creation", None
        )
        if not callable(continuation) or stateless_creation_generation is None:
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        try:
            continued = await continuation(
                owner,
                generation=stateless_creation_generation,
                expected_runtime_incarnation=expected_runtime_incarnation,
            )
        except Exception:
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        return EnsureResult(
            EnsureOutcome.PENDING,
            status="creating" if continued else current_status,
        )

    if authority == "exact_terminal":
        prepare = getattr(provisioner, "prepare_stateless_workspace_recreation", None)
        if not callable(prepare):
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        try:
            generation = await prepare(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                mode="create",
            )
        except Exception:
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        if not isinstance(generation, str):
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        return await _create(
            owner,
            provisioner,
            stateless_creation_generation=generation,
            allow_stateless_create=True,
        )

    if authority == "exact_absent" and stateless_creation_generation is not None:
        finalize = getattr(
            provisioner, "finalize_stateless_workspace_recreation_deletion", None
        )
        if callable(finalize):
            try:
                cleared = await finalize(
                    owner,
                    generation=stateless_creation_generation,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
            except Exception:
                cleared = False
            if cleared is True:
                return await _create(
                    owner,
                    provisioner,
                    stateless_creation_generation=stateless_creation_generation,
                    allow_stateless_create=True,
                )

    # exact_absent, replacement, unknown, and malformed results all retain the
    # cached runtime fence and retry without mutating Kubernetes or DB context.
    return EnsureResult(EnsureOutcome.PENDING, status=current_status)


async def _restore_snapshot_runtime(
    owner: "WorkspaceOwner",
    *,
    provisioner,
    suspension,
    current_status: Optional[str],
    expected_runtime_incarnation: str,
    stateless_creation_generation: str | None,
) -> "EnsureResult":
    """Restore only through the exact cached Pod incarnation.

    A failed strict extraction deliberately leaves the fresh Pod UID and
    restore marker cached for retry.  Probe that UID before any name-based
    restore effect: a Kubernetes 404 or same-name replacement does not prove
    the cached process stopped and must not be adopted.  The suspension service
    repeats the exact-live probe and skips name-based create/adoption when the
    UID is reused, closing the probe-to-restore race.
    """

    authority_probe = getattr(provisioner, "workspace_pod_authority", None)
    if not callable(authority_probe):
        return EnsureResult(EnsureOutcome.PENDING, status=current_status)
    try:
        authority = await authority_probe(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
    except Exception:
        return EnsureResult(EnsureOutcome.PENDING, status=current_status)

    if authority == "exact_live":
        if stateless_creation_generation is not None:
            continuation = getattr(
                provisioner, "continue_stateless_workspace_creation", None
            )
            if not callable(continuation):
                return EnsureResult(EnsureOutcome.PENDING, status=current_status)
            try:
                await continuation(
                    owner,
                    generation=stateless_creation_generation,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
            except Exception:
                pass
            # The exact create Ready CAS removes its marker. Snapshot extract
            # starts only on the next serialized pass, when the complete
            # binding/endpoint tuple can be parsed as strict restore authority.
            return EnsureResult(EnsureOutcome.PENDING, status="creating")
        restored = await suspension.restore(
            owner,
            expected_runtime_incarnation=expected_runtime_incarnation,
        )
        return EnsureResult(
            EnsureOutcome.PENDING,
            status="restoring" if restored else "failed",
        )

    if authority == "exact_terminal":
        prepare = getattr(provisioner, "prepare_stateless_workspace_recreation", None)
        if not callable(prepare):
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        try:
            generation = await prepare(
                owner,
                expected_runtime_incarnation=expected_runtime_incarnation,
                mode="restore",
            )
        except Exception:
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        if not isinstance(generation, str):
            return EnsureResult(EnsureOutcome.PENDING, status=current_status)
        restored = await suspension.restore(
            owner,
            stateless_creation_generation=generation,
            allow_stateless_create=True,
        )
        return EnsureResult(
            EnsureOutcome.PENDING,
            status="restoring" if restored else "failed",
        )

    if authority == "exact_absent" and stateless_creation_generation is not None:
        finalize = getattr(
            provisioner, "finalize_stateless_workspace_recreation_deletion", None
        )
        if callable(finalize):
            try:
                cleared = await finalize(
                    owner,
                    generation=stateless_creation_generation,
                    expected_runtime_incarnation=expected_runtime_incarnation,
                )
            except Exception:
                cleared = False
            if cleared is True:
                restored = await suspension.restore(
                    owner,
                    stateless_creation_generation=stateless_creation_generation,
                    allow_stateless_create=True,
                )
                return EnsureResult(
                    EnsureOutcome.PENDING,
                    status="restoring" if restored else "failed",
                )

    # exact_absent, replacement, unknown, and malformed values retain the
    # marker and cached UID without any create, restore, or delete effect.
    return EnsureResult(EnsureOutcome.PENDING, status=current_status)


async def ensure_workspace(
    owner: "WorkspaceOwner",
    *,
    provisioner,
    suspension,
    current_status: Optional[str],
    expected_runtime_incarnation: Optional[str] = None,
    require_runtime_incarnation: bool = False,
    snapshot_restore_required: Any = False,
    snapshot_restore_marker_present: bool = False,
    stateless_creation_generation: str | None = None,
    allow_stateless_create: bool = False,
    stateless_creation_refused: bool = False,
    pinned_runtime_lock_held: bool = False,
) -> "EnsureResult":
    """Idempotently drive owner's workspace toward 'ready'. Owner-agnostic
    extraction of the job dispatcher's container branch (main.py).

    Behavior notes (intentional):
    * 'failed' is owner-aware: SESSIONS self-heal (recreate); JOBS surface FAILED
      so the dispatcher fails the job — preserving the original dispatcher behavior
      (a job with a failed workspace fails; it does not silently retry forever).
    * 'suspended' kicks off restore as a FIRE-AND-FORGET task (restore is slow:
      pod create + SSH snapshot extract) and returns PENDING — matching the
      original `asyncio.create_task(restore_workspace(...))`; awaiting would block
      the dispatcher loop.
    * A stateless physical workspace with a cached runtime UID uses the
      provisioner's five-state Pod authority. The exact live UID is adopted;
      the exact terminal UID is deleted with a UID precondition and recreated;
      absence, replacement, and ambiguity fail closed without effects.
    * Legacy/non-incarnation 'ready' rows retain their phase-only liveness
      probe for compatibility.
    """
    s = current_status
    if stateless_creation_refused:
        return EnsureResult(EnsureOutcome.PENDING, status=s)
    if snapshot_restore_marker_present and type(snapshot_restore_required) is not bool:
        # Present lifecycle sentinels are exact JSON booleans. Treating null,
        # zero, an empty container, or a truthy string as absence would let a
        # background ensure create/adopt a workspace while restore debt is
        # malformed and still unresolved.
        return EnsureResult(EnsureOutcome.PENDING, status=s)
    if (
        require_runtime_incarnation
        and snapshot_restore_required is True
        and s in _RESTORE_REQUIRED_STATUSES
    ):
        # Soft retirement captures a snapshot and deletes the pod. A PVC may
        # reattach its live tree (the suspension service detects and skips the
        # extract), while an emptyDir replacement must unroll that snapshot.
        # Never let any durable restore intent fall through to a plain create
        # or Ready response that could advertise an empty/partial workspace.
        if expected_runtime_incarnation:
            return await _restore_snapshot_runtime(
                owner,
                provisioner=provisioner,
                suspension=suspension,
                current_status=s,
                expected_runtime_incarnation=expected_runtime_incarnation,
                stateless_creation_generation=stateless_creation_generation,
            )
        if stateless_creation_generation is None:
            return EnsureResult(EnsureOutcome.PENDING, status=s)
        restored = await suspension.restore(
            owner,
            stateless_creation_generation=stateless_creation_generation,
            allow_stateless_create=allow_stateless_create,
        )
        return EnsureResult(
            EnsureOutcome.PENDING,
            status="restoring" if restored else "failed",
        )
    if require_runtime_incarnation and expected_runtime_incarnation:
        return await _ensure_existing_runtime(
            owner,
            provisioner=provisioner,
            current_status=s,
            expected_runtime_incarnation=expected_runtime_incarnation,
            stateless_creation_generation=stateless_creation_generation,
        )
    if s in (None, "", "deleted", "none"):
        # No live workspace → (re)create one (both kinds).
        if require_runtime_incarnation and stateless_creation_generation is None:
            return EnsureResult(EnsureOutcome.PENDING, status=s)
        return await _create(
            owner,
            provisioner,
            stateless_creation_generation=stateless_creation_generation,
            allow_stateless_create=allow_stateless_create,
            pinned_runtime_lock_held=pinned_runtime_lock_held,
        )
    if require_runtime_incarnation and s in ("suspended", "restoring"):
        # Stateless restore is authorized only by the exact-true marker handled
        # above.  A phase string alone cannot authorize snapshot extraction or
        # name-based creation.
        return EnsureResult(EnsureOutcome.PENDING, status=s)
    if s == "failed":
        if owner.kind == "session":
            if require_runtime_incarnation and stateless_creation_generation is None:
                return EnsureResult(EnsureOutcome.PENDING, status=s)
            return await _create(
                owner,
                provisioner,
                stateless_creation_generation=stateless_creation_generation,
                allow_stateless_create=allow_stateless_create,
                pinned_runtime_lock_held=pinned_runtime_lock_held,
            )
        return EnsureResult(EnsureOutcome.FAILED, status="failed")
    if s == "suspended":
        asyncio.create_task(suspension.restore(owner))
        return EnsureResult(EnsureOutcome.PENDING, status="restoring")
    # A required-runtime session must be able to recover when the original
    # create/adopt waiter was interrupted after it stamped an in-progress
    # state. Re-entering _create is idempotent: it adopts the deterministic
    # live pod (or recreates a missing one), waits for readiness, and publishes
    # the authoritative Pod UID. The single-flight caller prevents local task
    # storms; provisioner idempotency covers HA replicas.
    if require_runtime_incarnation and s in ("created", "creating", "pending"):
        if stateless_creation_generation is None:
            return EnsureResult(EnsureOutcome.PENDING, status=s)
        return await _create(
            owner,
            provisioner,
            stateless_creation_generation=stateless_creation_generation,
            allow_stateless_create=allow_stateless_create,
            pinned_runtime_lock_held=pinned_runtime_lock_held,
        )
    if owner.kind == "session" and s == "pending":
        # Pinned K8s creates publish a durable multi-resource attempt before the
        # first API effect.  An SSH/readiness timeout deliberately leaves that
        # marker pending; re-enter the exact attempt instead of treating it as a
        # self-progressing phase with no recovery owner.
        return await _create(
            owner,
            provisioner,
            pinned_runtime_lock_held=pinned_runtime_lock_held,
        )
    # "Already progressing" set — keep in sync with the NOT IN clause in
    # PostgresDB.list_threads_needing_workspace (database/postgres.py).
    if s in ("created", "creating", "restoring", "suspending"):
        return EnsureResult(EnsureOutcome.PENDING, status=s)
    if s == "ready":
        # Legacy drift check. Required-runtime rows returned through the exact
        # authority path above; this phase-only boolean is intentionally never
        # used to authorize their replacement.
        probe = getattr(provisioner, "workspace_pod_live", None)
        if probe is not None:
            if require_runtime_incarnation:
                live = False
            else:
                live = await probe(owner)
            if live is False:
                if (
                    require_runtime_incarnation
                    and stateless_creation_generation is None
                ):
                    return EnsureResult(EnsureOutcome.PENDING, status=s)
                return await _create(
                    owner,
                    provisioner,
                    stateless_creation_generation=stateless_creation_generation,
                    allow_stateless_create=allow_stateless_create,
                    pinned_runtime_lock_held=pinned_runtime_lock_held,
                )
        return EnsureResult(EnsureOutcome.READY, status="ready")
    # Unknown / unexpected status — wait (the dispatcher skips and retries).
    return EnsureResult(EnsureOutcome.PENDING, status=s)
