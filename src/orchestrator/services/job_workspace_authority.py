"""Workspace authority for a job about to be delivered to a worker.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``R_WORKSPACE``). Two closely-coupled concerns live here because they call each
other:

1. **Attestation and the pre-delivery recheck.** ``attest_pinned_k8s_job_workspace``
   replaces a stored Kubernetes endpoint with a freshly server-attested one and
   refuses if the runtime incarnation or the endpoint moved;
   ``pinned_k8s_job_workspace_authority_is_current`` revalidates that exact
   tuple at the last network boundary; the two stateless attestations turn any
   provisioner failure into one generic claim refusal;
   ``workspace_runtime_unchanged_before_delivery`` re-reads the durable row
   after slow bundle assembly and requires the selected runtime digest to be
   unchanged. **Every one of these fails closed.** They exist to stop a bundle
   built against one pod from being delivered against another, so none of them
   may be relaxed to make a fixture or a test pass.

2. **Subjob workspace inheritance.** ``resolve_subjob_inherited_workspace``
   overlays a parent's *live* workspace onto an inheriting child, waits on a
   bounded budget while the parent is still provisioning, and fails with a
   diagnosable message when the parent's workspace can never become ready.
   ``fail_subjob_and_unblock_parent`` is its counterpart: failing a subjob at
   dispatch time is not enough, because the parent held in ``waiting`` is only
   released by the completion-side unblock handlers, which a dispatch-time
   failure never reaches. The pair must unblock a parent **exactly once** — the
   stateless CAS guard below is what keeps a stale dispatcher snapshot from
   unblocking a parent off a failure that never committed.

``services.job_workspace_adoption`` remains the authority for legacy Kubernetes
adoption; this module calls it, it does not reimplement it.
``shared.workspace_contract`` remains the authority for the tier contract and
the runtime digest.
"""

from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping, Protocol
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceRuntimeAttestation,
    WorkspaceRuntimeAuthorityError,
    WorkspaceRuntimeRecoveryRequired,
)
from orchestrator.services.job_workspace_adoption import (
    ensure_legacy_k8s_job_runtime_authority,
    verify_adopted_k8s_runtime_before_delivery,
)
from orchestrator.services.job_workspace_runtime import (
    get_container_context,
    stateless_worker_workspace_owner,
)
from orchestrator.services.workspace_lifecycle import EnsureOutcome, WorkspaceOwner
from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
    resolve_workspace_runtime,
    workspace_runtime_authority_digest,
)
from shared.workspace_recovery import WorkspaceRecoveryCode


class RecoverableWorkspaceAuthorityRefusal(HTTPException):
    """Preserve an explicit recovery reason across the authority boundary."""

    def __init__(self, recovery_code: WorkspaceRecoveryCode) -> None:
        super().__init__(409, "Stateless worker workspace authority unavailable")
        self.recovery_code = recovery_code


# Bounded wait for a subjob to inherit its parent's provisioned workspace.
# Parent container/VM readiness is an async event that lands AFTER the subjob is
# spawned (a scholar is created ~3s after its parent, mid-provisioning), so we
# resolve from the parent's live row every dispatch tick. This bounds how long
# we wait before giving up with a diagnosable failure instead of stranding the job.
INHERIT_WORKSPACE_MAX_WAIT_S = int(
    os.environ.get("WORKSPACE_INHERIT_MAX_WAIT_S", "600")
)


class JobWorkspaceAuthorityStore(Protocol):
    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    async def update_job_status(self, job_id: str, **kwargs: Any) -> Any: ...

    async def merge_job_context(self, job_id: str, patch: dict[str, Any]) -> Any: ...

    def acquire(self) -> Any: ...


class WorkspaceRuntimeAttestor(Protocol):
    async def attest_workspace_runtime(
        self, owner: WorkspaceOwner
    ) -> WorkspaceRuntimeAttestation: ...


@dataclass(frozen=True)
class JobWorkspaceAuthorityDependencies:
    """Per-invocation collaborators for job workspace authority.

    ``resolve_inherited_workspace``, ``fail_subjob_and_unblock_parent`` and
    ``workspace_runtime_unchanged_before_delivery`` are dependency fields even
    though this module defines all three: the application still owns the seams
    its other dispatch paths (and their tests) steer, and a module-local call
    would silently bypass a patched one (port contract §P3). Each is consumed
    only by a *different* function here, so the indirection cannot recurse.
    """

    store: JobWorkspaceAuthorityStore
    logger: logging.Logger
    workspace_provisioner: WorkspaceRuntimeAttestor
    vm_provisioner: Any
    vm_mode: Callable[[], Any]
    ensure_workspace: Callable[..., Awaitable[Any]]
    workspace_suspension: Any
    handle_scholar_completion: Callable[..., Awaitable[Any]]
    handle_delegation_child_completion: Callable[..., Awaitable[Any]]
    resolve_inherited_workspace: Callable[
        [dict[str, Any]], Awaitable[tuple[str, str | None]]
    ]
    fail_subjob_and_unblock_parent: Callable[[dict, str], Awaitable[None]]
    workspace_runtime_unchanged_before_delivery: Callable[
        [dict[str, Any]], Awaitable[bool]
    ]


@dataclass(frozen=True, slots=True)
class PinnedK8sJobWorkspaceAuthority:
    """One exact server-attested Kubernetes workspace delivery tuple."""

    owner: WorkspaceOwner
    attestation: WorkspaceRuntimeAttestation


# =============================================================================
# Attestation and the pre-delivery recheck
# =============================================================================


async def attest_pinned_k8s_job_workspace(
    job: dict[str, Any],
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> tuple[dict[str, Any], PinnedK8sJobWorkspaceAuthority | None]:
    """Replace a pinned job's Kubernetes endpoint with a fresh attestation."""

    decision = resolve_workspace_runtime(job, vm_mode=dependencies.vm_mode())
    if not decision.ready or decision.effective_backend != "sandbox":
        return job, None
    workspace = get_container_context(job)
    provisioner = str(workspace.get("provisioner") or "").strip().lower()
    if provisioner == "docker":
        return job, None
    if provisioner != "k8s":
        raise WorkspaceRuntimeAuthorityError(
            "sandbox workspace provisioner authority is unavailable"
        )
    try:
        expected_runtime = str(
            UUID(str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)))
        )
        stored_port = int(workspace.get("port") or 30022)
    except (TypeError, ValueError) as exc:
        raise WorkspaceRuntimeAuthorityError(
            "sandbox workspace runtime authority is malformed"
        ) from exc

    owner = stateless_worker_workspace_owner(job)
    attestation = await dependencies.workspace_provisioner.attest_workspace_runtime(
        owner
    )
    if attestation.runtime_incarnation != expected_runtime:
        raise WorkspaceRuntimeAuthorityError(
            "sandbox workspace runtime changed before delivery"
        )
    stored_host = workspace.get("host")
    stored_ip = workspace.get("pod_ip")
    if (
        (stored_host and str(stored_host) != attestation.host)
        or (stored_ip and str(stored_ip) != attestation.pod_ip)
        or stored_port != attestation.port
    ):
        raise WorkspaceRuntimeAuthorityError(
            "sandbox workspace endpoint changed before delivery"
        )

    exact_workspace = copy.deepcopy(workspace)
    exact_workspace.update(
        {
            "status": "ready",
            "provisioner": "k8s",
            "host": attestation.host,
            "pod_ip": attestation.pod_ip,
            "port": attestation.port,
            WORKSPACE_RUNTIME_INCARNATION_KEY: attestation.runtime_incarnation,
        }
    )
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError) as exc:
            raise WorkspaceRuntimeAuthorityError(
                "sandbox workspace context is malformed"
            ) from exc
    if not isinstance(context, Mapping):
        raise WorkspaceRuntimeAuthorityError("sandbox workspace context is malformed")
    exact_job = copy.deepcopy(job)
    exact_context = copy.deepcopy(dict(context))
    exact_context["workspace_container"] = exact_workspace
    exact_job["context"] = exact_context
    return exact_job, PinnedK8sJobWorkspaceAuthority(owner, attestation)


async def pinned_k8s_job_workspace_authority_is_current(
    durable_job: dict[str, Any],
    authority: PinnedK8sJobWorkspaceAuthority | None,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> bool:
    """Revalidate the exact workspace tuple at the final network boundary."""

    if not await dependencies.workspace_runtime_unchanged_before_delivery(durable_job):
        return False
    if authority is None:
        return True
    try:
        latest = await dependencies.store.get_job(str(durable_job["id"]))
        if latest is None:
            return False
        expected_owner = stateless_worker_workspace_owner(latest)
        if expected_owner != authority.owner:
            return False
        confirmed = await dependencies.workspace_provisioner.attest_workspace_runtime(
            expected_owner
        )
        return confirmed == authority.attestation
    except Exception:
        return False


async def attest_stateless_worker_workspace(
    owner: WorkspaceOwner,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> WorkspaceRuntimeAttestation:
    """Return one exact worker workspace identity or a generic claim refusal."""

    try:
        return await dependencies.workspace_provisioner.attest_workspace_runtime(owner)
    except WorkspaceRuntimeAuthorityError as exc:
        dependencies.logger.warning(
            "Stateless worker workspace attestation refused for %s %s: %s",
            owner.kind,
            owner.id,
            exc,
        )
    except Exception as exc:
        dependencies.logger.warning(
            "Stateless worker workspace attestation failed for %s %s: %s",
            owner.kind,
            owner.id,
            exc,
            exc_info=True,
        )
    raise HTTPException(
        status_code=409,
        detail="Stateless worker workspace authority unavailable",
    )


async def attest_stateless_worker_vm_workspace(
    owner: WorkspaceOwner,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> WorkspaceRuntimeAttestation:
    """Return one exact same-cluster VM identity or a generic refusal."""

    try:
        if owner.kind != "job":
            raise WorkspaceRuntimeAuthorityError(
                "stateless worker VM owner is not a job"
            )
        return await dependencies.vm_provisioner.attest_workspace_runtime(owner.id)
    except WorkspaceRuntimeRecoveryRequired as exc:
        raise RecoverableWorkspaceAuthorityRefusal(exc.recovery_code) from exc
    except WorkspaceRuntimeAuthorityError as exc:
        dependencies.logger.warning(
            "Stateless worker VM workspace attestation refused for %s %s: %s",
            owner.kind,
            owner.id,
            exc,
        )
    except Exception as exc:
        dependencies.logger.warning(
            "Stateless worker VM workspace attestation failed for %s %s: %s",
            owner.kind,
            owner.id,
            exc,
            exc_info=True,
        )
    raise HTTPException(
        status_code=409,
        detail="Stateless worker workspace authority unavailable",
    )


async def workspace_runtime_unchanged_before_delivery(
    job: dict[str, Any],
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> bool:
    """Recheck the selected runtime after slow bundle assembly.

    Provisioner callbacks can race config/model/credential resolution.  Refetch
    the durable job at the last network boundary and require the exact selected
    runtime authority to match what the bundle was built from.  Opposite-tier
    residue is deliberately excluded from the digest, so it is observable but
    can neither block nor replace the assigned runtime.
    """

    expected = workspace_runtime_authority_digest(job, vm_mode=dependencies.vm_mode())
    if expected is None:
        return False
    fresh = await dependencies.store.get_job(str(job["id"]))
    if fresh is None:
        return False
    context = fresh.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            return False
    if isinstance(context, dict) and context.get("inherits_parent_workspace"):
        action, _reason = await dependencies.resolve_inherited_workspace(fresh)
        if action != "proceed":
            return False
    if (
        workspace_runtime_authority_digest(fresh, vm_mode=dependencies.vm_mode())
        != expected
    ):
        return False
    return await verify_adopted_k8s_runtime_before_delivery(
        dependencies.store, dependencies.workspace_provisioner, fresh
    )


# =============================================================================
# Subjob workspace inheritance
# =============================================================================


async def resolve_subjob_inherited_workspace(
    job: dict,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> tuple[str, str | None]:
    """Refresh a subjob's inherited workspace from its parent's LIVE context.

    Subjobs that share a parent's workspace (scholar research,
    verification/critic) persist only ``inherits_parent_workspace`` and a
    parent-inheritance workspace contract. We re-read the parent's current
    workspace here and overlay it onto the in-memory ``job['context']``. The
    live runtime is never copied into the child row: doing so both creates a
    stale snapshot and claims the parent's Kubernetes authority under the
    child's identity. The subjob rides the parent's pod via its
    ``worktree_path``; it must never provision its own.

    Returns one of:
      ``("proceed", None)`` — not an inheriting subjob, or the parent workspace
        is ready and now overlaid; let normal dispatch continue.
      ``("wait", None)`` — inheriting, parent workspace still provisioning;
        caller should skip this tick and retry.
      ``("fail", message)`` — inheriting, but the parent workspace is gone/failed
        or the wait budget is exhausted; caller should fail the job.
    """
    parent_id = job.get("parent_job_id")
    if not parent_id:
        return ("proceed", None)

    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, ValueError):
            return ("proceed", None)

    # Discriminate INHERITED from SELF-PROVISIONED. Both eventually carry a
    # workspace_container/vm on their context — an inheriting subjob copies its
    # parent's at spawn; a self-provisioned subjob (parent had no workspace when
    # it was spawned) writes its OWN once its pod comes up. Only true inheritors
    # carry the explicit inherits_parent_workspace flag, stamped by
    # _spawn_scholar_subjob / _trigger_verification_on_complete when they copy the
    # parent snapshot. Gating on key *presence* (as this once did) misread a
    # self-provisioned scholar as inheriting and waited the full budget on a
    # parent workspace that never exists, then failed. See
    # knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md.
    if not ctx.get("inherits_parent_workspace"):
        return ("proceed", None)

    adoption = await ensure_legacy_k8s_job_runtime_authority(
        dependencies.store, dependencies.workspace_provisioner, job
    )
    if adoption.retryable:
        dependencies.logger.warning(
            "Dispatcher: subjob %s parent workspace needs live Kubernetes "
            "adoption (%s); waiting without dispatch",
            job.get("id"),
            adoption.reason,
        )
        return ("wait", None)
    if adoption.reason == "authority_ambiguous":
        return ("fail", "Inherited workspace authority is ambiguous.")

    try:
        parent = (
            adoption.authority_job
            if adoption.owner is not None and adoption.owner.id == str(parent_id)
            else await dependencies.store.get_job(str(parent_id))
        )
    except Exception as e:
        dependencies.logger.warning(
            "Dispatcher: subjob %s — failed to read parent %s for workspace "
            "resolution: %s (waiting)",
            job.get("id"),
            parent_id,
            e,
        )
        return ("wait", None)

    if not parent:
        return (
            "fail",
            f"Parent job {parent_id} no longer exists; cannot inherit its workspace.",
        )

    parent_ctx = parent.get("context") or {}
    if isinstance(parent_ctx, str):
        try:
            parent_ctx = json.loads(parent_ctx)
        except (json.JSONDecodeError, ValueError):
            parent_ctx = {}
    parent_container = parent_ctx.get("workspace_container") or {}
    parent_vm = parent_ctx.get("vm") or {}
    try:
        parent_contract = resolve_workspace_contract(parent)
    except WorkspaceContractError as exc:
        return ("fail", f"Parent workspace contract is invalid ({exc.code}).")
    # Resolve against a temporary overlay only. A waiting child keeps its
    # original durable-looking snapshot untouched; a genuine pre-0175 child
    # can still inherit the newly adopted parent UID without teaching the pure
    # resolver to trust its old endpoint.
    resolved_child_ctx = dict(ctx)
    if parent_contract.assigned_backend == "sandbox":
        resolved_child_ctx["workspace_container"] = parent_container
        resolved_child_ctx.pop("vm", None)
    elif parent_contract.assigned_backend == "vm":
        resolved_child_ctx["vm"] = parent_vm
        resolved_child_ctx.pop("workspace_container", None)
    try:
        child_contract = resolve_workspace_contract(
            {**job, "context": resolved_child_ctx}
        )
    except WorkspaceContractError as exc:
        return ("fail", f"Child workspace contract is invalid ({exc.code}).")
    if parent_contract.assigned_backend != child_contract.assigned_backend:
        return (
            "fail",
            "Parent and child workspace assignments differ; refusing cross-tier "
            f"inheritance ({parent_contract.assigned_backend} -> "
            f"{child_contract.assigned_backend}).",
        )
    inherited_backend = child_contract.assigned_backend

    # Parent's workspace is ready → overlay the live context and dispatch. All
    # downstream machinery (job_needs_sandbox/job_needs_vm, the dispatch-time
    # injectors) then keys off the ready context and injects workspace.remote.
    if inherited_backend == "sandbox" and parent_container.get("status") == "ready":
        ctx["workspace_container"] = parent_container
        ctx.pop("vm", None)
        job["context"] = ctx
        dependencies.logger.info(
            "Dispatcher: subjob %s inheriting parent %s sandbox runtime — "
            "resolved live at dispatch",
            job.get("id"),
            parent_id,
        )
        return ("proceed", None)
    if inherited_backend == "vm" and parent_vm.get("status") == "ready":
        ctx["vm"] = parent_vm
        ctx.pop("workspace_container", None)
        job["context"] = ctx
        dependencies.logger.info(
            "Dispatcher: subjob %s inheriting parent %s VM runtime — resolved "
            "live at dispatch",
            job.get("id"),
            parent_id,
        )
        return ("proceed", None)

    # Parent workspace is dead (reaped/failed) or the parent itself reached a
    # terminal state — no point waiting on a workspace that will never be ready.
    dead_states = ("failed", "deleted")
    parent_status = parent.get("status")
    if (
        (
            inherited_backend == "sandbox"
            and parent_container.get("status") in dead_states
        )
        or (inherited_backend == "vm" and parent_vm.get("status") in dead_states)
        or parent_status in ("failed", "cancelled", "completed")
    ):
        return (
            "fail",
            (
                f"Parent job {parent_id} workspace is unavailable (parent "
                f"status={parent_status}, container="
                f"{parent_container.get('status')}, vm={parent_vm.get('status')}); "
                "subjob cannot inherit it."
            ),
        )

    # Still provisioning — bounded wait keyed on the subjob's age, re-anchored
    # on the outage's scheduled wake for a resumed subjob: an outage-paused
    # subjob re-dispatched hours after spawn has long exhausted a created_at
    # budget, and would insta-fail on any transiently non-ready parent
    # workspace at resume. Anchor = max(created_at, llm_outage.next_retry_at)
    # — the same next-wake philosophy as the outage reset window.
    # knowledge-base/knowledge/features/llm_outage_subjob_resilience.md (#5)
    ref: datetime | None = None
    created_at = job.get("created_at")
    if isinstance(created_at, datetime):
        ref = (
            created_at if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        )
    wake_raw = (ctx.get("llm_outage") or {}).get("next_retry_at")
    if isinstance(wake_raw, str):
        try:
            wake = datetime.fromisoformat(wake_raw)
            if wake.tzinfo is None:
                wake = wake.replace(tzinfo=timezone.utc)
            if ref is None or wake > ref:
                ref = wake
        except ValueError:
            pass
    age_s = 0.0
    if ref is not None:
        age_s = (datetime.now(timezone.utc) - ref).total_seconds()
    if age_s > INHERIT_WORKSPACE_MAX_WAIT_S:
        return (
            "fail",
            (
                f"Timed out after {int(age_s)}s waiting for parent job "
                f"{parent_id} workspace to become ready (container="
                f"{parent_container.get('status')}, vm={parent_vm.get('status')})."
            ),
        )
    return ("wait", None)


async def prepare_job_workspace_runtime(
    job: dict[str, Any],
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> tuple[str, dict[str, Any], str | None]:
    """Converge historical K8s authority before any dispatch decision.

    Automatic dispatch, direct/manual start, resume and stateless claim bundles
    all enter here. Inherited jobs delegate adoption to their exact parent
    owner through ``resolve_subjob_inherited_workspace``.
    """

    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            context = {}
    if isinstance(context, dict) and context.get("inherits_parent_workspace"):
        action, reason = await dependencies.resolve_inherited_workspace(job)
        return action, job, reason

    adoption = await ensure_legacy_k8s_job_runtime_authority(
        dependencies.store, dependencies.workspace_provisioner, job
    )
    if adoption.retryable:
        return "wait", job, adoption.reason
    if (
        adoption.owner is not None
        and adoption.owner.id == str(job.get("id"))
        and adoption.authority_job is not None
    ):
        job = dict(adoption.authority_job)
    return "proceed", job, adoption.reason


async def fail_subjob_and_unblock_parent(
    job: dict,
    message: str,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> None:
    """Fail a subjob at dispatch time AND unblock the parent it was holding.

    The dispatcher can decide a subjob will never run (e.g. it cannot inherit its
    parent's workspace). Marking it ``failed`` is not enough: a parent held in
    ``waiting`` by ``_spawn_scholar_subjob`` — or a delegation parent — is only
    transitioned back to ``created`` by the completion-side unblock handlers,
    which run inside ``complete_job``, a path a dispatch-time failure never
    reaches. Without this the parent strands in ``waiting`` forever (secondary
    bug in
    knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md).
    Mirror ``complete_job``'s terminal-subjob unblock here so any dispatch-path
    failure is self-healing, not just today's inherit-timeout.
    """
    job_id = str(job["id"])
    stateless_worker = job.get("execution_lane") == "stateless"
    updated = await dependencies.store.update_job_status(
        job_id,
        status="failed",
        error_message=message,
        expected_status=(str(job.get("status")) if stateless_worker else None),
    )
    if stateless_worker and not updated:
        # The dispatcher row is a stale snapshot. A control verb may have
        # cancelled/completed the child while workspace inheritance was being
        # resolved; never overwrite that winner or unblock the parent from a
        # failure disposition that did not commit.
        dependencies.logger.info(
            "Dispatcher: skipped stale stateless subjob failure for %s "
            "(expected_status=%s)",
            job_id,
            job.get("status"),
        )
        return
    # The unblock handlers classify the outcome from job['status']; the in-memory
    # row still holds the pre-fail status, so sync it before delegating. Each
    # handler is a no-op for the wrong subjob type (scholar_target / creation_order
    # guards), so calling both is safe.
    job["status"] = "failed"
    for handler in (
        dependencies.handle_scholar_completion,
        dependencies.handle_delegation_child_completion,
    ):
        try:
            await handler(job, [])
        except Exception as e:
            dependencies.logger.error(
                "Dispatcher: subjob %s failed but could not unblock its parent "
                "via %s: %s",
                job_id,
                getattr(handler, "__name__", repr(handler)),
                e,
                exc_info=True,
            )


async def provision_parent_workspace_for_scholar(
    job: dict,
    parent_id: str,
    *,
    dependencies: JobWorkspaceAuthorityDependencies,
) -> str:
    """Drive the PARENT's shared workspace container toward ready on a scholar's
    behalf, then promote the scholar to a normal inheriting subjob.

    Phase 1 (knowledge-base/knowledge/issues/scholar_selfprovisioned_workspace_misclassified_as_inherited.md):
    a scholar spawned before its parent had any workspace provisions the parent's
    ONE shared pod (``workspace-<parentId>``, owner = parent) instead of a
    throwaway pod of its own, so the parent and later the critic ride the same
    pod. ``create_workspace`` keys the pod name and the context write-back on the
    owner, so provisioning under ``WorkspaceOwner.job(parent_id)`` lands the ready
    host/pod_ip on the PARENT's row automatically — no copy-back needed.

    Returns:
      ``"wait"``     — parent workspace still provisioning; retry next tick.
      ``"promoted"`` — parent workspace ready; the scholar row now inherits it and
                       dispatches via the normal inherit path on its next tick.
      ``"fail"``     — provisioning failed; the scholar was failed and its parent
                       unblocked via ``fail_subjob_and_unblock_parent``.
    """
    scholar_id = str(job["id"])
    parent = await dependencies.store.get_job(parent_id)
    if not parent:
        await dependencies.fail_subjob_and_unblock_parent(
            job,
            f"Parent job {parent_id} no longer exists; cannot provision its "
            "shared workspace for the research phase.",
        )
        return "fail"

    parent_ctx = parent.get("context") or {}
    if isinstance(parent_ctx, str):
        try:
            parent_ctx = json.loads(parent_ctx)
        except (json.JSONDecodeError, ValueError):
            parent_ctx = {}
    parent_container = parent_ctx.get("workspace_container") or {}

    res = await dependencies.ensure_workspace(
        WorkspaceOwner.job(parent_id),
        provisioner=dependencies.workspace_provisioner,
        suspension=dependencies.workspace_suspension,
        current_status=parent_container.get("status"),
    )
    if res.outcome is EnsureOutcome.FAILED:
        # create_workspace records the concrete failure in the parent's
        # context before returning False — surface it on the job row instead
        # of punting operators to the orchestrator logs (the dispatcher's own
        # sandbox arm already does this; see the EnsureOutcome.FAILED branch
        # in _try_dispatch_pending_jobs).
        reason = None
        try:
            refreshed_parent = await dependencies.store.get_job(parent_id)
            refreshed_ctx = (refreshed_parent or {}).get("context") or {}
            if isinstance(refreshed_ctx, str):
                refreshed_ctx = json.loads(refreshed_ctx)
            reason = (refreshed_ctx.get("workspace_container") or {}).get("error")
        except Exception:
            dependencies.logger.warning(
                "Dispatcher: could not refresh failed parent workspace context "
                "for scholar %s (parent %s)",
                scholar_id,
                parent_id,
                exc_info=True,
            )
        detail = (
            f"Shared parent workspace failed: {reason}"
            if reason
            else (
                "Shared parent workspace could not be created for the research "
                "phase (see orchestrator logs for image/resource/RBAC details)."
            )
        )
        await dependencies.fail_subjob_and_unblock_parent(job, detail)
        return "fail"
    if res.outcome is EnsureOutcome.PENDING:
        dependencies.logger.info(
            "Dispatcher: scholar %s provisioning shared parent workspace "
            "workspace-%s (status=%s) — waiting",
            scholar_id,
            parent_id[:12],
            res.status,
        )
        return "wait"

    # READY — promote the scholar to inherit the now-ready shared workspace so
    # the normal inherit path (fresh parent resolver overlay + worktree injection)
    # dispatches it. The child row must not copy the parent's live runtime
    # authority; doing so looks like an unreserved runtime bind for the child.
    fresh_parent = await dependencies.store.get_job(parent_id)
    fresh_ctx = (fresh_parent or {}).get("context") or {}
    if isinstance(fresh_ctx, str):
        try:
            fresh_ctx = json.loads(fresh_ctx)
        except (json.JSONDecodeError, ValueError):
            fresh_ctx = {}
    ready_container = fresh_ctx.get("workspace_container") or parent_container

    worktree_path = (
        "/home/agent-host/workspace/worktrees/"
        f"{scholar_id[:8]}-{job.get('config_name') or 'scholar'}"
    )
    await dependencies.store.merge_job_context(
        scholar_id,
        {"inherits_parent_workspace": True},
    )
    async with dependencies.store.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET worktree_path = $1 WHERE id = $2::uuid",
            worktree_path,
            scholar_id,
        )
    dependencies.logger.info(
        "Dispatcher: scholar %s promoted to inherit shared parent workspace "
        "workspace-%s (host=%s)",
        scholar_id,
        parent_id[:12],
        ready_container.get("host") or ready_container.get("pod_ip"),
    )
    return "promoted"
