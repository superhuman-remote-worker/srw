"""Session-side workspace provisioning + reconcile — the dispatcher-equivalent
for persistent sessions. See knowledge-history/done/unified_workspace_provisioning.md.

Jobs get workspace reconcile from the main dispatcher loop; sessions never did,
so a workspace wedged at 'failed'/missing for an active session never recovered.
This module closes that gap with an idempotent ensure + a periodic safety-net.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any, Optional
from uuid import uuid4

from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_CREATION_KEY,
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from orchestrator.services.session_runtime_admission import (
    ThreadRuntimeAuthority,
    same_thread_runtime_authority,
    thread_runtime_authority,
)
from orchestrator.services.workspace_binding import (
    ensure_virtual_thread_workspace_binding,
)
from orchestrator.services.workspace_lifecycle import (
    EnsureOutcome,
    EnsureResult,
    WorkspaceOwner,
    ensure_workspace,
)
from orchestrator.services.workspace_suspension import (
    WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY,
)
from shared.backend_kinds import LITE_BACKENDS, VM_BACKENDS

logger = logging.getLogger(__name__)


def _thread_metadata(thread: dict) -> dict:
    md = thread.get("metadata") or {}
    if isinstance(md, str):
        # md is a non-empty string here ("" was already coerced to {} above).
        try:
            md = json.loads(md)
        except (json.JSONDecodeError, TypeError):
            md = {}
    return md if isinstance(md, dict) else {}


def _ws_status(thread: dict) -> Optional[str]:
    return (_thread_metadata(thread).get("workspace_container") or {}).get("status")


def _thread_backend(thread: dict) -> Optional[str]:
    """The thread's configured ``workspace.backend`` (stored in
    ``metadata.config_override``), or None when unset."""
    co = _thread_metadata(thread).get("config_override") or {}
    if not isinstance(co, dict):
        return None
    return (co.get("workspace") or {}).get("backend")


async def ensure_session_workspace(
    thread_id: str,
    *,
    db,
    provisioner,
    suspension,
    _workspace_lifecycle_lock_held: bool = False,
    expected_runtime_generation: str | None = None,
    _pinned_runtime_lock_held: bool = False,
) -> Optional[EnsureResult]:
    """Idempotently drive a session's *container* workspace toward ready.

    Returns None when there is nothing for this function to provision: the thread
    is gone or ended, or its tier owns no workspace container — ``virtual``/
    ``none`` (no workspace pod at all) and ``vm`` (the workspace is the VM in
    ``metadata.vm``). Only container-backed tiers reach ``ensure_workspace``."""
    thread = await db.get_thread(thread_id)
    if not thread or thread.get("status") == "ended":
        return None

    pinned_authority: ThreadRuntimeAuthority | None = None
    if thread.get("execution_lane") == "pinned":
        pinned_authority = thread_runtime_authority(thread)
        if pinned_authority is None:
            return None
        if (
            expected_runtime_generation is not None
            and pinned_authority.generation != expected_runtime_generation
        ):
            return None
        expected_runtime_generation = pinned_authority.generation
        if not _pinned_runtime_lock_held:
            lock_impl = getattr(type(db), "thread_advisory_lock", None)
            if callable(lock_impl):
                async with lock_impl(db, thread_id):
                    return await ensure_session_workspace(
                        thread_id,
                        db=db,
                        provisioner=provisioner,
                        suspension=suspension,
                        _workspace_lifecycle_lock_held=(_workspace_lifecycle_lock_held),
                        expected_runtime_generation=expected_runtime_generation,
                        _pinned_runtime_lock_held=True,
                    )

    if thread.get("execution_lane") == "stateless":
        _, refusal = stateless_session_workspace_check(thread)
        if refusal is not None:
            logger.error(
                "Stateless workspace ensure refused for thread %s: %s",
                thread_id,
                refusal,
            )
            return EnsureResult(EnsureOutcome.PENDING, status=_ws_status(thread))

    # Suspended-VM restore on reconnect — the VM-tier mirror of
    # ensure_workspace's 'suspended' branch below. VM suspend deletes the VM
    # with purge_disk=False; `rootdisk == "kept"` with the VM torn down is the
    # durable signature of "the disk is waiting for a resume". (Not
    # vm.status == "suspended": suspend writes that marker, but the
    # controller's async delete-status overwrites it with 'deleted'.) Without
    # this trigger nothing restores a suspended VM session at all — the two
    # restore triggers in main.py key on workspace_container.status, which VM
    # suspend never writes (live-gate finding, thread a1240add).
    #
    # Checked BEFORE the backend arms because an UPGRADED thread still declares
    # its original backend ('virtual'/'sandbox' — the upgrade endpoints never
    # rewrite it), so the declared string cannot be allowed to hide the disk.
    # Safe from the reconcile sweep: it selects status='active' threads only,
    # and a suspended thread is 'suspended' — this fires from /prepare
    # (reconnect) alone. Double-fire converges: restore sets vm.status=
    # 'restoring' immediately, which this condition excludes.
    vm_ctx = _thread_metadata(thread).get("vm") or {}
    if vm_ctx.get("rootdisk") == "kept" and vm_ctx.get("status") in (
        "suspended",
        "deleted",
        "deleting",
    ):
        logger.info(
            "session %s has a kept VM rootdisk (vm.status=%s) — restoring the VM",
            thread_id,
            vm_ctx.get("status"),
        )
        await suspension.restore(
            WorkspaceOwner.session(thread_id),
            _pinned_runtime_lock_held=_pinned_runtime_lock_held,
        )
        return EnsureResult(EnsureOutcome.PENDING, status="restoring")

    backend = _thread_backend(thread)
    if backend in LITE_BACKENDS:
        # virtual/none sessions run with no workspace pod (no_workspace_agent_mode.md
        # §4) — nothing to provision or reconcile. Centralized here so both the
        # resume path and the periodic reconcile sweep skip lite threads.
        logger.debug(
            "session %s uses a lite workspace backend — no workspace to provision",
            thread_id,
        )
        if backend == "virtual":
            await ensure_virtual_thread_workspace_binding(db, thread_id)
        return None
    if backend in VM_BACKENDS:
        # A vm-tier session's workspace IS the VM (metadata.vm, provisioned at
        # create by vm_provisioner.create_thread_vm and reconciled by the VM
        # lifecycle manager). Provisioning a sandbox container alongside it makes
        # the agent attach to the container instead — it wins the readiness race
        # by minutes — so the session silently runs on the wrong tier while the
        # VM is orphaned. Centralized here rather than at the call site because
        # THREE paths reach this function: /prepare (routers/sessions.py),
        # resume, and the periodic reconcile sweep. The sweep is an independent
        # trigger: _setup_gitea writes workspace_container={git_remote_url,
        # repo_name} for every thread including vm-tier ones, and that
        # status-less entry matches list_threads_needing_workspace's filter.
        # knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md (Defect 1)
        logger.debug(
            "session %s is vm-tier — workspace is the VM, no container to provision",
            thread_id,
        )
        return None
    workspace_context = _thread_metadata(thread).get("workspace_container") or {}
    requires_runtime_attestation = bool(
        thread.get("execution_lane") == "stateless"
        and backend == "sandbox"
        and workspace_context.get("provisioner") == "k8s"
    )

    # Every stateless physical entry (input poll, resume/prepare, and periodic
    # reconcile) reaches this function.  Serialize the whole create/adopt or
    # snapshot-restore effect across orchestrator replicas, then re-read under
    # ownership so a loser observes the winner's durable Ready state instead
    # of extracting the same snapshot twice.  Test doubles only participate
    # when their class explicitly implements the production lock protocol;
    # AsyncMock's dynamic attributes must not be mistaken for one.
    lock_impl = getattr(type(db), "stateless_session_workspace_ensure_lock", None)
    if (
        requires_runtime_attestation
        and not _workspace_lifecycle_lock_held
        and callable(lock_impl)
    ):
        async with lock_impl(db, thread_id) as owner:
            if not owner:
                return EnsureResult(
                    EnsureOutcome.PENDING,
                    status=workspace_context.get("status"),
                )
            return await ensure_session_workspace(
                thread_id,
                db=db,
                provisioner=provisioner,
                suspension=suspension,
                _workspace_lifecycle_lock_held=True,
            )

    result = await ensure_workspace(
        WorkspaceOwner.session(thread_id),
        provisioner=provisioner,
        suspension=suspension,
        current_status=_ws_status(thread),
        expected_runtime_incarnation=(
            workspace_context.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
            if requires_runtime_attestation
            else None
        ),
        # Stateless physical attach treats the Pod UID as part of shell
        # authority. A legacy Ready row with no UID must converge through the
        # same idempotent create/adopt path as a missing pod.
        require_runtime_incarnation=requires_runtime_attestation,
        # Exact-boolean lifecycle sentinel: only JSON true authorizes restore.
        # Passing raw preserves malformed present values so the lifecycle
        # service can fail closed instead of truthiness-coercing them.
        snapshot_restore_required=workspace_context.get(
            WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY
        ),
        snapshot_restore_marker_present=(
            WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY in workspace_context
        ),
        **await _stateless_creation_arguments(
            thread_id,
            db=db,
            workspace_context=workspace_context,
            requires_runtime_attestation=requires_runtime_attestation,
            lifecycle_lock_held=_workspace_lifecycle_lock_held,
        ),
        pinned_runtime_lock_held=_pinned_runtime_lock_held,
    )

    if pinned_authority is not None:
        fresh = await db.get_thread(thread_id)
        if not same_thread_runtime_authority(fresh, pinned_authority):
            fresh_metadata = _thread_metadata(fresh or {})
            fresh_workspace = fresh_metadata.get("workspace_container") or {}
            runtime_uid = fresh_workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
            if runtime_uid:
                await provisioner.delete_workspace(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=str(runtime_uid),
                    wait_for_exact_absence=True,
                )
            return None

    if requires_runtime_attestation:
        # Terminal cleanup can race an already-started Kubernetes create. The
        # public lifecycle first makes the durable thread ended; after
        # actuation, give up any pod created for a row that is now gone or
        # terminal. Suspended is deliberately *not* a give-up state here: the
        # same ensure call may just have restored its workspace for a queued
        # wake. This is the cross-replica backstop that prevents post-delete
        # pod/PVC resurrection.
        fresh = await db.get_thread(thread_id)
        fresh_status = fresh.get("status") if fresh else None
        if fresh is None or fresh_status == "ended":
            release = getattr(provisioner, "release_workspace", None)
            if callable(release):
                await release(
                    WorkspaceOwner.session(thread_id),
                    reclaim_volume=fresh is None,
                )
            return None

    return result


async def _stateless_creation_arguments(
    thread_id: str,
    *,
    db,
    workspace_context: dict,
    requires_runtime_attestation: bool,
    lifecycle_lock_held: bool,
) -> dict:
    """Recover or claim the one-shot Pod-create authority for one ensure.

    Every production stateless Kubernetes ensure is serialized by the shared
    lifecycle advisory lock before reaching this helper.  The marker is
    persisted before Kubernetes actuation.  This helper reports whether it is
    still unattempted; the provisioner performs the durable false-to-true CAS
    immediately before the sole Pod-create call.  An attempted marker is
    read/adopt-only on every later retry.
    """

    if not requires_runtime_attestation:
        return {}
    if WORKSPACE_RUNTIME_CREATION_KEY not in workspace_context:
        if workspace_context.get(WORKSPACE_RUNTIME_INCARNATION_KEY) is not None:
            # A completed Ready runtime (or a failed strict extract on that
            # exact runtime) has no outstanding Pod-create authority. Its
            # UID-fenced ready/restore path does not need a create marker.
            return {}
        # Never manufacture provenance for a markerless physical row. Pod
        # actuation precedes UID publication, so even a 404 may hide an old
        # partitioned process. Fresh create and Resume pre-arm the marker at
        # their own durable authority transitions.
        return {"stateless_creation_refused": True}
    prepare_impl = getattr(
        type(db), "prepare_stateless_thread_workspace_creation", None
    )
    if not lifecycle_lock_held or not callable(prepare_impl):
        logger.error(
            "Stateless workspace creation refused without lifecycle/DB authority "
            "for thread %s",
            thread_id,
        )
        return {"stateless_creation_refused": True}

    raw_restore = workspace_context.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY, False)
    if (
        WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY in workspace_context
        and type(raw_restore) is not bool
    ):
        return {"stateless_creation_refused": True}
    mode = "restore" if raw_restore is True else "create"
    try:
        plan = await prepare_impl(
            db,
            thread_id,
            proposed_generation=str(uuid4()),
            mode=mode,
        )
    except Exception:
        logger.exception(
            "Failed to prepare stateless workspace creation for thread %s",
            thread_id,
        )
        return {"stateless_creation_refused": True}
    if not isinstance(plan, dict) or plan.get("state") not in {"pending", "runtime"}:
        return {"stateless_creation_refused": True}
    marker = plan.get("creation")
    if marker is None:
        return {}
    if not isinstance(marker, dict):
        return {"stateless_creation_refused": True}
    generation = marker.get("generation")
    attempted = marker.get("attempted")
    if not isinstance(generation, str) or type(attempted) is not bool:
        return {"stateless_creation_refused": True}
    return {
        "stateless_creation_generation": generation,
        # This is only candidacy. ContainerProvisioner performs the durable
        # false->true attempt CAS after PVC/seed preparation and immediately
        # before the one permitted Pod-create call.
        "allow_stateless_create": attempted is False,
    }


async def reconcile_session_workspaces(*, db, provisioner, suspension) -> int:
    """Safety-net: re-ensure workspaces for active sessions whose workspace
    container exists but is not ready (e.g. 'failed'). Idempotent — ensure_workspace
    no-ops in-progress states. Returns the count of threads re-ensured. Never raises."""
    try:
        threads = await db.list_threads_needing_workspace()
    except Exception:
        logger.exception("session reconcile: failed to list threads needing workspace")
        return 0
    ensured = 0
    for t in threads:
        tid = t["id"] if isinstance(t, dict) else t
        try:
            res = await ensure_session_workspace(
                str(tid), db=db, provisioner=provisioner, suspension=suspension
            )
            if res is not None:
                ensured += 1
        except Exception:
            logger.exception("session reconcile: ensure failed for thread %s", tid)
    if ensured:
        logger.info("session reconcile: re-ensured %d thread workspace(s)", ensured)
    return ensured


async def workspace_idle_sweeper(
    shutdown_event: asyncio.Event,
    *,
    store,
    provisioner,
    suspension,
    vm_idle_service_factory: Callable[[], Any] | None = None,
    terminal_vm_controls_factory: Callable[[], Any] | None = None,
) -> None:
    """Reconcile session recovery and durable stateless VM idle operations.

    Idle suspension and teardown now live in the lifecycle reconciler's reap
    path (``services/lifecycle/reconciler.py`` → ``WorkspaceInstanceManager``),
    which snapshots-then-deletes reapable workspaces and force-deletes ones it
    can never reach (bounded retry) instead of keeping them alive forever.

    Session recovery remains independent of idle policy. When migration 0270
    exists, the bounded VM Job release/wake adapter built by
    ``vm_idle_service_factory`` also runs here every 60 seconds, including
    operations admitted before the new-release flag was disabled. The Job
    controls built by ``terminal_vm_controls_factory`` replay terminal Job VM
    cleanup in a background task the sweep never waits on.
    """
    logger.info("Workspace idle sweeper started")
    vm_idle_service = None
    if vm_idle_service_factory is not None:
        async with store.acquire() as idle_conn:
            idle_schema = await idle_conn.fetchval(
                "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
            )
        if idle_schema:
            vm_idle_service = vm_idle_service_factory()
    terminal_vm_controls = (
        terminal_vm_controls_factory()
        if terminal_vm_controls_factory is not None
        else None
    )
    terminal_vm_task: asyncio.Task[int] | None = None
    try:
        while not shutdown_event.is_set():
            # Session workspace reconcile (safety-net): recreate failed/missing
            # workspaces for active sessions. Runs regardless of whether idle
            # suspension is enabled — recovering a wedged workspace is independent
            # of idle policy. This is the session-side equivalent of the job
            # dispatcher's per-cycle workspace reconcile.
            # (reconcile_session_workspaces never raises; the try/except is a
            # belt-and-suspenders guard so a future change can't kill this loop.)
            try:
                await reconcile_session_workspaces(
                    db=store,
                    provisioner=provisioner,
                    suspension=suspension,
                )
            except Exception as e:
                logger.error("Error in session workspace reconcile: %s", e)

            if vm_idle_service is not None:
                try:
                    await vm_idle_service.reconcile_once(limit=16)
                except Exception:
                    logger.exception("VM idle reconcile held")

            # Terminal Job rows outlive HTTP callers. Replay their ordinary exact
            # VM cleanup through the same admission after a timeout or restart;
            # retained terminal reviews remain owned by vm_idle_service above.
            if terminal_vm_controls is not None and (
                terminal_vm_task is None or terminal_vm_task.done()
            ):
                if terminal_vm_task is not None:
                    try:
                        terminal_vm_task.result()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        logger.exception("Terminal Job VM cleanup reconcile held")
                terminal_vm_task = asyncio.create_task(
                    terminal_vm_controls.reconcile_terminal_vm_cleanups(limit=4)
                )

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
                break
            except asyncio.TimeoutError:
                pass

    finally:
        if terminal_vm_task is not None:
            terminal_vm_task.cancel()
            try:
                await terminal_vm_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Terminal Job VM cleanup reconcile held on shutdown")
    logger.info("Workspace idle sweeper stopped")
