"""Thread End and stateless/pinned retirement application operations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from functools import partial
import json
import logging
import os
import time
from typing import Any, Literal

from fastapi import HTTPException
import httpx

from orchestrator.database.postgres import OfficerPostLifecycleConflict
from orchestrator.services import (
    officer_post_lifecycle as officer_post_lifecycle_service,
)
from orchestrator.services.cloud import SessionFolderHandle
from orchestrator.services.cloud_stage_authority import (
    _capture_cloud_stage_authority,
    _retirement_stage_event_from_receipt as retirement_stage_event_from_receipt,
)
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceCleanupOutcome,
    WorkspaceRuntimeAuthorityError,
)
from orchestrator.services.managed_repository_authority import (
    revoke_and_delete_managed_repository,
)
from orchestrator.services.pinned_retirement import PinnedRetirementOperations
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.session_runtime_admission import thread_runtime_authority
from orchestrator.services.stateless_workspace_gate import (
    declared_thread_workspace_backend,
    stateless_session_workspace_check,
    thread_metadata_object,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from shared.pinned_session_identity import PinnedSessionBinding


@dataclass(frozen=True, slots=True)
class ThreadRetirementDependencies:
    """Application state and explicit cross-domain lifecycle ports."""

    store: Any
    agent_provisioner: Any
    persistent_provisioner: Any
    container_provisioner: Any
    vm_provisioner: Any
    recovery_store: Any
    docker_provisioner: Any
    workspace_suspension_service: Any
    snapshot_service: Any
    gitea_client: Any
    main_cloud_router: Any
    pinned_retirement: PinnedRetirementOperations
    build_agent_cloud_mount: Callable[..., Awaitable[Any]]
    get_container_context: Callable[[dict[str, Any]], Any]
    get_vm_context: Callable[[dict[str, Any]], Any]
    vm_needs_release: Callable[..., bool]
    thread_uses_pinned_execution: Callable[[Any], bool]
    threads_suspending: set[str]
    require_stateless_end_workspace: Callable[..., Any]
    decommission_officer_post: Callable[..., Awaitable[Any]]
    conclude_conference_if_any: Callable[..., Awaitable[None]]
    logger: logging.Logger


async def thread_turn_in_flight(
    thread: dict[str, Any],
    *,
    dependencies: ThreadRetirementDependencies,
) -> bool:
    """Return idle only after the exact pinned runtime attests it.

    A recycled Pod IP, an old replica, or a failed probe is not an idle
    observation.  Unknown therefore fails closed as ``True``; callers may use
    force-End when they intentionally accept that uncertainty.
    """

    postgres_db = dependencies.store

    agent_id = thread.get("agent_id")
    if not agent_id:
        return False
    try:
        context = thread.get("runtime_retirement_context") or {}
        if isinstance(context, str):
            context = json.loads(context)
        retirement_token = str(thread.get("runtime_retirement_token") or "")
        if retirement_token and isinstance(context, Mapping):
            captured_agent = context.get("agent")
            if not isinstance(captured_agent, Mapping):
                return True
            captured_agent_pod = context.get("agent_pod")
            if not isinstance(captured_agent_pod, Mapping):
                return True
            binding = PinnedSessionBinding(
                thread_id=str(thread.get("id") or context.get("thread_id") or ""),
                runtime_generation=str(
                    thread.get("runtime_generation") or context.get("generation") or ""
                ),
                agent_id=str(captured_agent.get("id") or ""),
                runtime_attach_token=str(context.get("runtime_attach_token") or ""),
                agent_hostname=str(captured_agent.get("hostname") or ""),
                pod_namespace=str(captured_agent_pod.get("namespace") or ""),
                pod_uid=str(captured_agent.get("pod_uid") or ""),
                pod_ip=str(captured_agent.get("pod_ip") or ""),
                pod_port=int(captured_agent.get("pod_port") or 8001),
                agent_status=str(captured_agent.get("status") or ""),
            )
        else:
            generation = str(thread.get("runtime_generation") or "")
            thread_id = str(thread.get("id") or "")
            if not generation or not thread_id:
                return True
            binding = await postgres_db.get_pinned_session_binding(
                thread_id,
                expected_runtime_generation=generation,
            )
            if binding is None or binding.agent_id != str(agent_id):
                return True
        url = f"http://{binding.pod_ip}:{binding.pod_port}/session/status"
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.post(
                url,
                json={
                    "session_identity_fingerprint": (
                        binding.session_identity_fingerprint
                    )
                },
            )
        if response.status_code != 200:
            return True
        payload = response.json()
        if not (
            isinstance(payload, Mapping)
            and payload.get("recipient_verified") is True
            and payload.get("session_identity_fingerprint")
            == binding.session_identity_fingerprint
            and str(payload.get("thread_id") or "") == binding.thread_id
            and isinstance(payload.get("turn_in_flight"), bool)
        ):
            return True

        if retirement_token:
            current = await postgres_db.get_thread(binding.thread_id)
            current_context = (current or {}).get("runtime_retirement_context") or {}
            if isinstance(current_context, str):
                current_context = json.loads(current_context)
            if not (
                current
                and str(current.get("runtime_generation") or "")
                == binding.runtime_generation
                and str(current.get("runtime_retirement_token") or "")
                == retirement_token
                and isinstance(current_context, Mapping)
                and str(current_context.get("runtime_attach_token") or "")
                == binding.runtime_attach_token
                and str((current_context.get("agent") or {}).get("id") or "")
                == binding.agent_id
            ):
                return True
        else:
            current_binding = await postgres_db.get_pinned_session_binding(
                binding.thread_id,
                expected_runtime_generation=binding.runtime_generation,
            )
            if (
                current_binding is None
                or current_binding.target_key != binding.target_key
            ):
                return True
        return payload["turn_in_flight"]
    except Exception:
        return True


def stateless_retirement_marker(thread: dict[str, Any]) -> dict[str, Any]:
    from shared.session_retirement import stateless_retirement_authority

    marker = stateless_retirement_authority(thread_metadata_object(thread))
    if marker is None:
        raise RuntimeError("stateless retirement marker is absent")
    return marker


async def reconcile_stateless_thread_retirement(
    thread_id: str,
    *,
    force: bool,
    permanent: bool,
    dependencies: ThreadRetirementDependencies,
) -> dict[str, Any]:
    """Converge one already-serialized stateless terminal lifecycle.

    Queue closure is the first durable effect. Claimant quiescence, exact
    remote shell retirement, snapshot/delete, and marker clearance then occur
    in that order. Every ambiguous boundary leaves the ended thread + closed
    queue marker intact so End or soft Resume can retry the same token.
    """

    postgres_db = dependencies.store
    agent_provisioner = dependencies.agent_provisioner
    container_provisioner = dependencies.container_provisioner
    workspace_suspension_service = dependencies.workspace_suspension_service
    logger = dependencies.logger
    _build_agent_cloud_mount = dependencies.build_agent_cloud_mount
    _stateless_retirement_marker = stateless_retirement_marker

    from orchestrator.services.stateless_session_retirement import (
        ShellRetirementUnavailable,
        retire_stateless_session_shell,
        retire_stateless_workspace_residents,
        verify_stateless_workspace_residents_retired,
    )
    from shared.session_retirement import (
        mark_session_claim_eviction_requested,
        stateless_retirement_release_authorized,
    )

    async def _rebase_stale_terminal_runtime_or_raise(
        *,
        terminal_token: int,
        retired_runtime_incarnation: str,
    ) -> None:
        """Retarget the pending fence, then require a fresh retirement pass."""

        rebased = await postgres_db.rebase_stateless_thread_workspace_retirement(
            thread_id,
            terminal_token=terminal_token,
            retired_runtime_incarnation=retired_runtime_incarnation,
            permanent=permanent,
        )
        if not rebased:
            raise HTTPException(
                status_code=503,
                detail="Terminal workspace successor authority is not yet safe",
            )
        raise HTTPException(
            status_code=503,
            detail="Terminal workspace authority advanced; retry retirement",
        )

    # A transitional/legacy Kubernetes row can lack its immutable Pod UID.
    # There is no safe absence proof for that shape: Pod creation precedes DB
    # binding/runtime publication, so even a backing-less 404 can follow a
    # crash plus force deletion while the partitioned process still runs.
    workspace_absence_proven = False
    preflight_thread = await postgres_db.get_thread(thread_id)
    if preflight_thread is None:
        return {"state": "missing"}
    preflight_metadata = thread_metadata_object(preflight_thread)
    preflight_workspace = preflight_metadata.get("workspace_container") or {}
    preflight_binding = preflight_metadata.get("_workspace_binding") or {}
    if isinstance(preflight_workspace, dict) and isinstance(preflight_binding, dict):
        preflight_backing = str(preflight_binding.get("backing_id") or "")
        preflight_physical = bool(
            preflight_workspace.get("provisioner") == "k8s"
            or preflight_backing.startswith("k8s-")
        )
        preflight_runtime = preflight_workspace.get("_runtime_incarnation")
        settled_present = (
            "_stateless_workspace_retirement_settled" in preflight_metadata
        )
        pending_authority = preflight_metadata.get("_stateless_claim_retirement")
        pending_retains_runtime = bool(
            isinstance(pending_authority, dict)
            and pending_authority.get("runtime_incarnation")
        )
        restore_marker_present = "_snapshot_restore_required" in preflight_workspace
        restore_required = preflight_workspace.get("_snapshot_restore_required", False)
        if restore_marker_present and type(restore_required) is not bool:
            raise HTTPException(
                status_code=503,
                detail="Stateless workspace restore authority is malformed",
            )
        creation_pending = "_runtime_creation" in preflight_workspace
        restore_pending = (
            preflight_thread.get("status") != "ended" and restore_required is True
        )
        # The durable creation marker is the only authority that may bridge a
        # committed Kubernetes create and its exact UID publication. Markerless
        # historical rows remain fail-closed.
        if (
            preflight_physical
            and not preflight_runtime
            and not settled_present
            and not pending_retains_runtime
            and not creation_pending
        ):
            raise HTTPException(
                status_code=503,
                detail=(
                    "Stateless workspace runtime identity is unavailable; "
                    "workspace reconciliation must publish an exact UID first"
                ),
            )
        if creation_pending or restore_pending:
            # Cancellation after the one-shot Pod call may leave a published
            # UID but no Ready binding/fingerprint. Terminal retirement cannot
            # infer those fields or mutate the thread to ended first: doing so
            # would make exact continuation reject the lifecycle forever.
            # A restore retry has a second edge: exact UID continuation can
            # publish Ready while deliberately leaving the snapshot debt for a
            # later extraction pass. Under the already-held distributed
            # lifecycle lock, drive one exact reconciliation pass and then
            # require *both* creation and restore authority to be clear before
            # Begin. If another pass is needed, leave thread and queue lifecycle
            # untouched and let the caller retry End.
            await ensure_session_workspace(
                thread_id,
                db=postgres_db,
                provisioner=container_provisioner,
                suspension=workspace_suspension_service,
                _workspace_lifecycle_lock_held=True,
            )
            preflight_thread = await postgres_db.get_thread(thread_id)
            if preflight_thread is None:
                return {"state": "missing"}
            _, creation_refusal = stateless_session_workspace_check(preflight_thread)
            preflight_metadata = thread_metadata_object(preflight_thread)
            preflight_workspace = preflight_metadata.get("workspace_container") or {}
            preflight_binding = preflight_metadata.get("_workspace_binding") or {}
            post_restore_present = (
                isinstance(preflight_workspace, dict)
                and "_snapshot_restore_required" in preflight_workspace
            )
            post_restore_required = (
                preflight_workspace.get("_snapshot_restore_required", False)
                if isinstance(preflight_workspace, dict)
                else None
            )
            post_restore_malformed = bool(
                post_restore_present and type(post_restore_required) is not bool
            )
            post_restore_pending = bool(
                preflight_thread.get("status") != "ended"
                and post_restore_required is True
            )
            if (
                creation_refusal is not None
                or not isinstance(preflight_workspace, dict)
                or preflight_workspace.get("status") != "ready"
                or "_runtime_creation" in preflight_workspace
                or post_restore_malformed
                or post_restore_pending
            ):
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Stateless workspace creation and restore must reach "
                        "exact Ready authority before retirement"
                    ),
                )

    async def _begin_retirement(*, requested_force: bool) -> dict[str, Any]:
        try:
            return await postgres_db.begin_stateless_thread_workspace_retirement(
                thread_id,
                force=requested_force,
                permanent=permanent,
                workspace_absence_proven=workspace_absence_proven,
            )
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail="Stateless retirement authority is malformed or changed",
            ) from exc

    closure = await _begin_retirement(requested_force=force)
    state = str(closure.get("state") or "")
    if state == "missing":
        return {"state": "missing"}
    if state == "busy":
        raise HTTPException(
            status_code=409,
            detail={"code": "stateless_end_busy", **closure},
        )
    if state in {
        "incompatible",
        "unsafe_missing_queue",
        "needs_runtime_preflight",
    }:
        raise HTTPException(
            status_code=409,
            detail={"code": "stateless_retirement_unsafe", **closure},
        )
    if state == "settled":
        if closure.get("permanent") is not permanent:
            raise HTTPException(
                status_code=503,
                detail="Settled stateless retirement intent is malformed",
            )
        if permanent:
            backing_id = closure.get("backing_id")
            runtime_incarnation = closure.get("runtime_incarnation")
            if isinstance(backing_id, str) and backing_id.startswith("k8s-"):
                # Soft End already proved process zero and removed the exact
                # Pod. Permanent upgrade creates a new terminal-reclaim
                # generation: preserve authority cannot authorize PVC deletion.
                if not runtime_incarnation:
                    raise HTTPException(
                        status_code=503,
                        detail=("Settled workspace runtime authority is unavailable"),
                    )
                cleanup_intent = (
                    await container_provisioner.prepare_workspace_cleanup_intent(
                        WorkspaceOwner.session(thread_id),
                        expected_runtime_incarnation=str(runtime_incarnation),
                        target_disposition="deleted",
                        reclaim_shared_resources=True,
                    )
                )
                if (
                    not isinstance(cleanup_intent, dict)
                    or cleanup_intent.get("resources_captured_at") is None
                    or cleanup_intent.get("reclaim_shared_resources") is not True
                ):
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Settled workspace permanent cleanup authority is incomplete"
                        ),
                    )
                cleanup = (
                    await container_provisioner.reconcile_workspace_cleanup_intent(
                        WorkspaceOwner.session(thread_id),
                        expected_runtime_incarnation=str(runtime_incarnation),
                        intent_generation=int(cleanup_intent["intent_generation"]),
                    )
                )
                if (
                    not isinstance(cleanup, WorkspaceCleanupOutcome)
                    or not cleanup.settled
                ):
                    raise HTTPException(
                        status_code=503,
                        detail="Settled workspace permanent cleanup is incomplete",
                    )
        return {
            "state": "settled",
            "thread": await postgres_db.get_thread(thread_id),
            "closure": closure,
        }
    if state != "closed":
        raise HTTPException(
            status_code=503,
            detail="Stateless retirement could not close its queue",
        )
    if closure.get("retry") and closure.get("permanent") is not permanent:
        raise HTTPException(
            status_code=409,
            detail="A different stateless retirement intent is still pending",
        )

    terminal_token = int(closure.get("terminal_token") or 0)
    deadline = time.monotonic() + float(
        os.environ.get("STATELESS_TERMINAL_CLAIM_ACK_TIMEOUT_S", "25")
    )
    while not closure.get("claimant_quiesced"):
        raw_losses = closure.get("claim_losses") or []
        if not isinstance(raw_losses, list) or not raw_losses or terminal_token <= 0:
            raise HTTPException(
                status_code=503,
                detail="Terminal claimant-loss authority is incomplete",
            )
        for raw_loss in raw_losses:
            if not isinstance(raw_loss, dict):
                raise HTTPException(
                    status_code=503,
                    detail="Terminal claimant-loss authority is malformed",
                )
            pod_name = str(raw_loss.get("pod") or "")
            pod_uid = str(raw_loss.get("pod_uid") or "")
            previous_token = int(raw_loss.get("lease_token") or 0)
            if not pod_name or not pod_uid or previous_token <= 0:
                raise HTTPException(
                    status_code=503,
                    detail="Terminal claimant identity is incomplete",
                )
            authority = await agent_provisioner.agent_pod_authority(
                pod_name,
                expected_pod_uid=pod_uid,
            )
            if authority == "exact_terminal":
                await postgres_db.acknowledge_stateless_thread_claimant_absent(
                    thread_id,
                    terminal_token=terminal_token,
                    previous_lease_token=previous_token,
                    previous_leased_by=pod_name,
                    previous_pod_uid=pod_uid,
                )
            elif authority == "exact_live":
                if await mark_session_claim_eviction_requested(
                    postgres_db,
                    thread_id=thread_id,
                    previous_lease_token=previous_token,
                    leased_by=pod_name,
                    pod_uid=pod_uid,
                ):
                    await agent_provisioner.delete_agent_pod_exact(
                        pod_name,
                        expected_pod_uid=pod_uid,
                    )
        closure = await _begin_retirement(requested_force=True)
        if closure.get("claimant_quiesced"):
            break
        if time.monotonic() >= deadline:
            raise HTTPException(
                status_code=503,
                detail="Terminal claimant quiescence is not yet acknowledged",
            )
        await asyncio.sleep(0.25)

    terminal_cloud_mount_cfg: dict[str, Any] | None = None
    if closure.get("resident_cleanup_required") and not closure.get(
        "resident_acknowledged"
    ):
        current = await postgres_db.get_thread(thread_id)
        if current is None:
            return {"state": "missing"}
        metadata = thread_metadata_object(current)
        workspace = metadata.get("workspace_container") or {}
        marker = _stateless_retirement_marker(current)
        runtime_incarnation = marker.get("runtime_incarnation")
        if not runtime_incarnation:
            raise HTTPException(
                status_code=503,
                detail="Resident cleanup runtime identity is unavailable",
            )
        runtime_authority = await container_provisioner.workspace_pod_authority(
            WorkspaceOwner.session(thread_id),
            expected_runtime_incarnation=str(runtime_incarnation),
        )
        if runtime_authority == "exact_terminal":
            # Exact UID plus all containers observed terminated proves both
            # shell and resident processes stopped. API-object absence alone
            # is not proof: a partitioned kubelet may keep the old process
            # running after a force deletion removes the object.
            acknowledged = await postgres_db.acknowledge_stateless_thread_shell_absent(
                thread_id,
                terminal_token=terminal_token,
                runtime_incarnation=str(runtime_incarnation),
            )
            if not acknowledged:
                raise HTTPException(
                    status_code=503,
                    detail="Absent resident retirement proof was not durable",
                )
        elif runtime_authority == "exact_live" and workspace.get("status") in {
            "ready",
            "retiring_process_zero",
        }:
            terminal_cloud_mount_cfg = await _build_agent_cloud_mount(
                current,
                mount_rows=await postgres_db.list_thread_mounts(thread_id),
                metadata=metadata,
                terminal_retirement_token=terminal_token,
            )
            try:
                proof = await retire_stateless_workspace_residents(
                    current,
                    terminal_token=terminal_token,
                    cloud_mount_cfg=terminal_cloud_mount_cfg,
                )
            except ShellRetirementUnavailable as exc:
                cause = exc.__cause__
                logger.warning(
                    "Stateless terminal cleanup remains pending: thread=%s "
                    "stage=resident workspace_status=%s cloud_mount=%s cause=%s",
                    thread_id,
                    workspace.get("status"),
                    terminal_cloud_mount_cfg is not None,
                    (
                        f"{type(cause).__name__}: {str(cause)[:300]}"
                        if cause is not None
                        else type(exc).__name__
                    ),
                )
                raise HTTPException(
                    status_code=503,
                    detail="Workspace resident retirement is not yet acknowledged",
                ) from exc
            # The resident protocol's final command independently proves the
            # exact workspace has no managed-repository credential agents.
            # Persist that existing generic process-zero authority before the
            # resident ACK. Otherwise release_workspace() must repeat the SSH
            # retirement after the terminal shell record has been written and
            # can remain fail-closed forever even though this exact proof was
            # already obtained.
            process_zero_recorded = (
                await postgres_db.record_managed_repository_workspace_process_zero(
                    thread_id,
                    owner_kind="thread",
                    scope="workspace_container",
                    provisioner="k8s",
                    runtime_incarnation=proof.authority.runtime_incarnation,
                )
            )
            if not process_zero_recorded:
                raise HTTPException(
                    status_code=503,
                    detail="Managed repository process-zero proof was not durable",
                )
            acknowledged = (
                await postgres_db.acknowledge_stateless_thread_resident_retirement(
                    thread_id,
                    terminal_token=terminal_token,
                    workspace_generation=proof.authority.workspace_generation,
                    endpoint_generation=proof.authority.workspace_generation,
                    runtime_incarnation=proof.authority.runtime_incarnation,
                    host_key_fingerprint=proof.authority.host_key_fingerprint,
                    proof=proof.as_dict(),
                )
            )
            if not acknowledged:
                raise HTTPException(
                    status_code=503,
                    detail="Workspace resident retirement acknowledgement was ambiguous",
                )
        elif runtime_authority == "exact_absent" and (
            await postgres_db.acknowledge_stateless_thread_runtime_process_zero(
                thread_id,
                terminal_token=terminal_token,
                runtime_incarnation=str(runtime_incarnation),
            )
        ):
            # The Pod left before these proofs, but only after its finalizer
            # release durably recorded the exact UID's container termination.
            # That receipt, required inside the acknowledging UPDATE, is the
            # evidence exact_terminal shows; bare absence still refuses below.
            pass
        else:
            raise HTTPException(
                status_code=503,
                detail="Workspace resident runtime authority changed or is ambiguous",
            )
        closure = await _begin_retirement(requested_force=True)

    if closure.get("shell_retirement_required") and not closure.get(
        "remote_acknowledged"
    ):
        current = await postgres_db.get_thread(thread_id)
        if current is None:
            return {"state": "missing"}
        metadata = thread_metadata_object(current)
        workspace = metadata.get("workspace_container") or {}
        marker = _stateless_retirement_marker(current)
        runtime_incarnation = marker.get("runtime_incarnation")
        if not runtime_incarnation:
            raise HTTPException(
                status_code=503,
                detail="Remote shell runtime identity is unavailable",
            )
        runtime_authority = await container_provisioner.workspace_pod_authority(
            WorkspaceOwner.session(thread_id),
            expected_runtime_incarnation=str(runtime_incarnation),
        )
        if runtime_authority == "exact_live" and workspace.get("status") in {
            "ready",
            "retiring_process_zero",
        }:
            if (
                closure.get("resident_cleanup_required")
                and terminal_cloud_mount_cfg is None
            ):
                terminal_cloud_mount_cfg = await _build_agent_cloud_mount(
                    current,
                    mount_rows=await postgres_db.list_thread_mounts(thread_id),
                    metadata=metadata,
                    terminal_retirement_token=terminal_token,
                )
            try:
                authority = await retire_stateless_session_shell(
                    current,
                    terminal_token=terminal_token,
                )
                if closure.get("resident_cleanup_required"):
                    # Shell kill is a second mutation boundary. Re-scan under
                    # the exact retired-T marker so a pane/background process
                    # cannot repopulate the snapshot after the pre-kill proof.
                    await verify_stateless_workspace_residents_retired(
                        current,
                        terminal_token=terminal_token,
                        cloud_mount_cfg=terminal_cloud_mount_cfg,
                    )
            except ShellRetirementUnavailable as exc:
                cause = exc.__cause__
                logger.warning(
                    "Stateless terminal cleanup remains pending: thread=%s "
                    "stage=shell_or_postcheck workspace_status=%s "
                    "cloud_mount=%s cause=%s",
                    thread_id,
                    workspace.get("status"),
                    terminal_cloud_mount_cfg is not None,
                    (
                        f"{type(cause).__name__}: {str(cause)[:300]}"
                        if cause is not None
                        else type(exc).__name__
                    ),
                )
                raise HTTPException(
                    status_code=503,
                    detail="Remote shell retirement is not yet acknowledged",
                ) from exc
            acknowledged = (
                await postgres_db.acknowledge_stateless_thread_shell_retirement(
                    thread_id,
                    terminal_token=terminal_token,
                    workspace_generation=authority.workspace_generation,
                    endpoint_generation=authority.workspace_generation,
                    runtime_incarnation=authority.runtime_incarnation,
                    host_key_fingerprint=authority.host_key_fingerprint,
                )
            )
        elif runtime_authority == "exact_terminal":
            acknowledged = await postgres_db.acknowledge_stateless_thread_shell_absent(
                thread_id,
                terminal_token=terminal_token,
                runtime_incarnation=str(runtime_incarnation),
            )
        elif runtime_authority == "exact_absent":
            # Same exact-receipt rule as the resident stage above.
            acknowledged = (
                await postgres_db.acknowledge_stateless_thread_runtime_process_zero(
                    thread_id,
                    terminal_token=terminal_token,
                    runtime_incarnation=str(runtime_incarnation),
                )
            )
        else:
            acknowledged = False
        if not acknowledged:
            raise HTTPException(
                status_code=503,
                detail="Remote shell retirement acknowledgement was ambiguous",
            )

    # The remote effects above and workspace destruction below are separate
    # authority boundaries.  Re-lock/re-read the terminal row before *any*
    # snapshot, Pod, Service, or PVC effect and require exact claimant,
    # resident, and shell proofs.  The shared parser also rejects malformed
    # JSON booleans/ACK tuples and post-ACK incarnation drift.
    closure = await _begin_retirement(requested_force=True)
    if str(closure.get("state") or "") != "closed":
        raise HTTPException(
            status_code=503,
            detail="Stateless retirement authority changed before workspace release",
        )

    current = await postgres_db.get_thread(thread_id)
    if current is None:
        return {"state": "missing"}
    metadata = thread_metadata_object(current)
    try:
        release_authority = stateless_retirement_release_authorized(metadata)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail="Stateless retirement proofs are incomplete or malformed",
        ) from exc
    if int(release_authority["terminal_token"]) != terminal_token:
        raise HTTPException(
            status_code=503,
            detail="Stateless retirement token changed before workspace release",
        )
    workspace = metadata.get("workspace_container") or {}
    marker = _stateless_retirement_marker(current)
    binding = metadata.get("_workspace_binding") or {}
    backing_id = str(binding.get("backing_id") or "")
    runtime_incarnation = marker.get("runtime_incarnation")
    runtime_authority = "not_applicable"
    if workspace.get("provisioner") == "k8s" and runtime_incarnation:
        runtime_authority = await container_provisioner.workspace_pod_authority(
            WorkspaceOwner.session(thread_id),
            expected_runtime_incarnation=str(runtime_incarnation),
        )
    physical_workspace = bool(
        workspace.get("provisioner") == "k8s"
        or backing_id.startswith("k8s-")
        or runtime_incarnation
    )
    if physical_workspace and not (
        backing_id.startswith("k8s-pod:") or backing_id.startswith("k8s-pvc:")
    ):
        raise HTTPException(
            status_code=503,
            detail="Stateless workspace backing authority is incomplete",
        )
    requires_snapshot = bool(not permanent and backing_id.startswith("k8s-pod:"))
    raw_snapshot_proof = workspace.get("_snapshot_restore_required", False)
    if type(raw_snapshot_proof) is not bool:
        raise HTTPException(
            status_code=503,
            detail="Stateless snapshot proof is malformed",
        )
    snapshot_already_captured = raw_snapshot_proof is True
    if runtime_authority in {"replacement", "unknown"}:
        raise HTTPException(
            status_code=503,
            detail="Stateless workspace runtime authority changed or is ambiguous",
        )
    if runtime_authority == "exact_absent":
        if requires_snapshot and not snapshot_already_captured:
            raise HTTPException(
                status_code=503,
                detail="Required emptyDir snapshot was not durably captured",
            )
        if permanent:
            cleanup_intent = (
                await container_provisioner.prepare_workspace_cleanup_intent(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=str(runtime_incarnation),
                    target_disposition="deleted",
                    reclaim_shared_resources=True,
                )
            )
            if (
                not isinstance(cleanup_intent, dict)
                or cleanup_intent.get("resources_captured_at") is None
                or cleanup_intent.get("reclaim_shared_resources") is not True
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Stateless absent workspace cleanup authority is incomplete",
                )
            cleanup = await container_provisioner.reconcile_workspace_cleanup_intent(
                WorkspaceOwner.session(thread_id),
                expected_runtime_incarnation=str(runtime_incarnation),
                intent_generation=int(cleanup_intent["intent_generation"]),
            )
            released = isinstance(cleanup, WorkspaceCleanupOutcome) and cleanup.settled
        else:
            released = await container_provisioner.release_absent_workspace(
                WorkspaceOwner.session(thread_id),
                reclaim_volume=False,
                expected_runtime_incarnation=str(runtime_incarnation),
                strict=True,
            )
        if not released:
            raise HTTPException(
                status_code=503,
                detail="Stateless absent workspace cleanup remains incomplete",
            )
    elif physical_workspace:
        if not runtime_incarnation:
            raise HTTPException(
                status_code=503,
                detail="Stateless workspace runtime identity is unavailable",
            )
        if runtime_authority == "exact_terminal":
            if requires_snapshot and not snapshot_already_captured:
                # An emptyDir whose runtime is already terminal cannot be
                # archived now. Do not destroy its object or clear retirement
                # authority without a previously committed snapshot proof.
                raise HTTPException(
                    status_code=503,
                    detail="Required emptyDir snapshot was not durably captured",
                )
            # Persist disposition and exact captured resource identities before
            # any Kubernetes effect. Permanent End cannot reuse preserve-only
            # authority to reclaim PVC/Service state.
            cleanup_intent = (
                await container_provisioner.prepare_workspace_cleanup_intent(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=str(runtime_incarnation),
                    target_disposition="deleted",
                    reclaim_shared_resources=permanent,
                )
            )
            if (
                not isinstance(cleanup_intent, dict)
                or cleanup_intent.get("resources_captured_at") is None
                or bool(cleanup_intent.get("reclaim_shared_resources")) is not permanent
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Terminal workspace cleanup authority is incomplete",
                )
            deletion = await container_provisioner.delete_workspace_with_outcome(
                WorkspaceOwner.session(thread_id),
                expected_runtime_incarnation=str(runtime_incarnation),
                wait_for_exact_absence=True,
                target_disposition="deleted",
                reclaim_shared_resources=permanent,
                cleanup_intent=cleanup_intent,
            )
            if deletion.stale_target_settled:
                await _rebase_stale_terminal_runtime_or_raise(
                    terminal_token=terminal_token,
                    retired_runtime_incarnation=str(runtime_incarnation),
                )
            if not deletion.current_deleted:
                raise HTTPException(
                    status_code=503,
                    detail="Terminal workspace Pod deletion remains incomplete",
                )
            cleanup = await container_provisioner.reconcile_workspace_cleanup_intent(
                WorkspaceOwner.session(thread_id),
                expected_runtime_incarnation=str(runtime_incarnation),
                intent_generation=int(cleanup_intent["intent_generation"]),
            )
            if not isinstance(cleanup, WorkspaceCleanupOutcome) or not cleanup.settled:
                raise HTTPException(
                    status_code=503,
                    detail="Terminal workspace cleanup remains incomplete",
                )
        elif runtime_authority != "exact_live":
            raise HTTPException(
                status_code=503,
                detail="Stateless workspace runtime identity is unavailable",
            )
        else:
            expected_runtime = str(runtime_incarnation)
            expected_fingerprint = (
                str(marker.get("host_key_fingerprint"))
                if marker.get("host_key_fingerprint")
                else None
            )

            # Begin may have pre-admitted the terminal cleanup generation in
            # the same transaction that closed the queue and projected
            # retiring_process_zero. Claim that exact generation and capture
            # its Pod/PVC/Service UIDs before release_workspace reads it.
            # Passing an admitted-but-uncaptured intent directly to the lower
            # deletion boundary correctly fails closed without issuing DELETE.
            try:
                teardown_identity = (
                    await container_provisioner.capture_terminal_workspace_identity(
                        WorkspaceOwner.session(thread_id)
                    )
                )
            except WorkspaceRuntimeAuthorityError as exc:
                raise HTTPException(
                    status_code=503,
                    detail="Live workspace teardown identity is unavailable",
                ) from exc
            if (
                teardown_identity.pod_uid != expected_runtime
                or not teardown_identity.pod_ip
                or teardown_identity.ssh_host_key_fingerprint != expected_fingerprint
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Live workspace teardown identity changed",
                )
            cleanup_intent = (
                await container_provisioner.prepare_workspace_cleanup_intent(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=expected_runtime,
                    target_disposition="deleted",
                    reclaim_shared_resources=permanent,
                    identity=teardown_identity,
                )
            )
            if (
                not isinstance(cleanup_intent, dict)
                or cleanup_intent.get("resources_captured_at") is None
                or bool(cleanup_intent.get("reclaim_shared_resources")) is not permanent
            ):
                raise HTTPException(
                    status_code=503,
                    detail="Live workspace cleanup authority is incomplete",
                )

            async def _snapshot_ack() -> bool:
                return (
                    await postgres_db.mark_stateless_thread_snapshot_restore_required(
                        thread_id,
                        terminal_token=terminal_token,
                    )
                )

            try:
                released = await asyncio.wait_for(
                    container_provisioner.release_workspace(
                        WorkspaceOwner.session(thread_id),
                        reclaim_volume=permanent,
                        require_snapshot=(
                            requires_snapshot and not snapshot_already_captured
                        ),
                        expected_runtime_incarnation=expected_runtime,
                        expected_host_key_fingerprint=expected_fingerprint,
                        on_snapshot_captured=(
                            _snapshot_ack
                            if requires_snapshot and not snapshot_already_captured
                            else None
                        ),
                        capture_snapshot=(
                            requires_snapshot and not snapshot_already_captured
                        ),
                        strict_terminal_snapshot=True,
                        strict=True,
                        teardown_identity=teardown_identity,
                    ),
                    timeout=float(
                        os.environ.get("STATELESS_TERMINAL_RELEASE_TIMEOUT_S", "300")
                    ),
                )
            except asyncio.TimeoutError as exc:
                raise HTTPException(
                    status_code=503,
                    detail="Stateless workspace retirement timed out",
                ) from exc
            if not released:
                raise HTTPException(
                    status_code=503,
                    detail="Stateless workspace retirement remains incomplete",
                )
    elif workspace.get("provisioner") == "k8s":
        # Missing context is not proof that the deterministic pod name is free.
        # A never-provisioned sandbox may finish only after Kubernetes confirms
        # no physical runtime exists; ambiguity keeps the marker retryable.
        released = await container_provisioner.release_absent_workspace(
            WorkspaceOwner.session(thread_id),
            reclaim_volume=permanent,
            strict=True,
        )
        if not released:
            raise HTTPException(
                status_code=503,
                detail="Stateless workspace absence could not be established",
            )

    if (
        not permanent
        and not await postgres_db.finish_stateless_thread_workspace_retirement(
            thread_id
        )
    ):
        raise HTTPException(
            status_code=503,
            detail="Stateless workspace retirement proofs did not settle",
        )
    return {"state": "settled", "thread": current, "closure": closure}


async def end_thread_flow(
    thread_id: str,
    thread: dict[str, Any],
    *,
    permanent: bool,
    force: bool,
    officer_retire_reason: str = "retired",
    officer_post_required: bool = False,
    include_officer_handoff: bool = False,
    expected_runtime_generation: str | None = None,
    expected_agent_id: str | None = None,
    expected_attach_token: str | None = None,
    require_expected_agent_offline: bool = False,
    settle_status: Literal["ended", "suspended"] = "ended",
    local_runtime_quiesced: bool = False,
    retiring_agent_response_pending: bool = False,
    require_physical_agent_stop: bool = False,
    dependencies: ThreadRetirementDependencies,
) -> dict[str, Any]:
    """The End funnel body — everything ``end_thread`` does after auth.

    Shared by the owner-facing DELETE and the project-admin officer
    decommission endpoint (officer_post.md §5): both must run the same
    stand-down, resource release, and status write, so there is exactly one
    way a thread leaves service. ``officer_retire_reason`` is recorded on
    the post's incarnation entry when the thread holds one ('retired' for a
    direct DELETE, 'decommissioned' via the endpoint). Direct End permits the
    transaction's orphan-only branch; the explicit project endpoint requires
    that its expected thread still holds the post. ``include_officer_handoff``
    is an internal response bridge for that endpoint, never a public request
    field.
    """

    postgres_db = dependencies.store
    container_provisioner = dependencies.container_provisioner
    vm_provisioner = dependencies.vm_provisioner
    snapshot_service = dependencies.snapshot_service
    gitea_client = dependencies.gitea_client
    main_cloud_router = dependencies.main_cloud_router
    logger = dependencies.logger
    pinned = dependencies.pinned_retirement
    _agent_pod_provision_intent_zero_candidate = (
        pinned.agent_pod_provision_intent_zero_candidate
    )
    _begin_pinned_thread_retirement = pinned.begin_pinned_thread_retirement
    _cleanup_pinned_thread_retirement = pinned.cleanup_pinned_thread_retirement
    _complete_retiring_soft_warm_binding_release = (
        pinned.complete_retiring_soft_warm_binding_release
    )
    _pinned_retirement_is_current = pinned.pinned_retirement_is_current
    _pre_registration_agent_pod_zero_candidate = (
        pinned.pre_registration_agent_pod_zero_candidate
    )
    _recover_agent_pod_provision_intent_zero = (
        pinned.recover_agent_pod_provision_intent_zero
    )
    _recover_pre_registration_agent_pod_zero = (
        pinned.recover_pre_registration_agent_pod_zero
    )
    _retirement_context_runtime_exposed = pinned.retirement_context_runtime_exposed
    _retirement_has_exact_local_quiescence = (
        pinned.retirement_has_exact_local_quiescence
    )
    _revoke_never_delivered_protected_reader = (
        pinned.revoke_never_delivered_protected_reader
    )
    _reconcile_stateless_thread_retirement = partial(
        reconcile_stateless_thread_retirement, dependencies=dependencies
    )
    _thread_turn_in_flight = partial(thread_turn_in_flight, dependencies=dependencies)
    _require_stateless_end_workspace = dependencies.require_stateless_end_workspace
    _decommission_officer_post = dependencies.decommission_officer_post
    _conclude_conference_if_any = dependencies.conclude_conference_if_any

    def _retirement_stage_event_from_receipt(
        retirement: Mapping[str, Any],
        current_thread: Mapping[str, Any],
        row: Mapping[str, Any] | None,
    ) -> tuple[bool, dict[str, Any] | None]:
        return retirement_stage_event_from_receipt(
            retirement,
            current_thread,
            row,
            never_delivered_protected_reader_shape=(
                pinned.never_delivered_protected_reader_shape
            ),
        )

    if permanent and settle_status != "ended":
        raise ValueError("permanent retirement cannot settle suspended")
    if require_physical_agent_stop and (
        permanent or settle_status != "suspended" or retiring_agent_response_pending
    ):
        raise ValueError("idle physical stop requires an orchestrator soft retirement")
    if retiring_agent_response_pending and not local_runtime_quiesced:
        raise ValueError("retiring agent response requires local quiescence")
    stateless = thread.get("execution_lane") == "stateless"
    initial_status = thread.get("status")
    initial_stateless_authority: dict[str, Any] | None = None

    if not stateless and permanent:
        terminal_join = await postgres_db.reserve_pinned_thread_idle_terminal_end(
            thread_id,
        )
        if terminal_join == "waiting_for_release":
            return {
                "status": "ending", "retirement_disposition": "ended",
                "retirement_permanent": True,
            }
        if terminal_join in {"wake_won", "held"}:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_idle_terminal_join_held"},
            )
        if terminal_join == "missing":
            return {"status": "deleted"}

    async def _stand_down(
        authoritative_thread: dict[str, Any],
        *,
        retirement_authority: Mapping[str, Any] | None = None,
        converge_authorized: bool = False,
    ) -> dict[str, Any] | None:
        """Run idempotent End-owned stand-down under durable authority.

        For the first linked Officer transition, ``retirement_authority`` is
        appended in the same database transaction as decommission after the
        transaction's in-flight refusal point. Every other caller authorizes
        before entering this helper. ``converge_authorized`` makes a retry of
        an irrevocable retirement ignore a newly observed Officer-job warning
        rather than returning a force prompt for work that is already ending.
        """

        authoritative_metadata = thread_metadata_object(authoritative_thread)
        officer_meta = (authoritative_metadata.get("config_override") or {}).get(
            "officer"
        ) or {}
        handoff: dict[str, Any] | None = None
        if isinstance(officer_meta, dict) and officer_meta.get("enabled") in (
            True,
            "true",
            "True",
            1,
        ):
            try:
                handoff = await _decommission_officer_post(
                    authoritative_thread,
                    reason=officer_retire_reason,
                    force=force or converge_authorized,
                    allow_orphan_retirement=not officer_post_required,
                    retirement=retirement_authority,
                )
            except OfficerPostLifecycleConflict as exc:
                raise HTTPException(status_code=409, detail=exc.detail) from exc
            # An enabled legacy/orphan thread may own no durable post. It must
            # still be disabled before ordinary End continues, but it may not
            # harvest over another incarnation. Current-post failures are NOT
            # swallowed: the authoritative method raises and End stays
            # visibly retryable.
            if handoff and handoff.get("blocked_by_in_flight"):
                return handoff
            if not handoff:
                disabled = await postgres_db.merge_thread_config_override(
                    thread_id, {"officer": {"enabled": False}}
                )
                if not disabled:
                    raise RuntimeError(
                        f"Officer stand-down could not disable thread {thread_id}"
                    )
        await _conclude_conference_if_any(authoritative_thread)
        return handoff

    async def _delete_auxiliary_state(
        authoritative_thread: dict[str, Any],
    ) -> None:
        """Best-effort external records removed only for permanent End."""

        authoritative_metadata = thread_metadata_object(authoritative_thread)
        authoritative_ws = authoritative_metadata.get("workspace_container") or {}
        if snapshot_service.is_available:
            try:
                fresh = await postgres_db.get_thread(thread_id) or {}
                for key in thread_metadata_object(fresh).get("log_archive_keys") or []:
                    await snapshot_service.delete_blob(str(key))
            except Exception as exc:
                logger.warning(
                    "Log archive cleanup failed for deleted thread %s: %s",
                    thread_id,
                    exc,
                )

        repo_name = authoritative_ws.get("repo_name")
        if repo_name:
            if not await revoke_and_delete_managed_repository(
                postgres_db, gitea_client, repo_name
            ):
                raise RuntimeError("Repository credential revocation is retryable")

        session_handle_str = authoritative_thread.get(
            "main_cloud_session_handle"
        ) or authoritative_thread.get("nc_session_folder")
        if session_handle_str:
            backend = main_cloud_router.for_thread(authoritative_thread)
            if backend.is_initialized:
                try:
                    session_handle = SessionFolderHandle.from_db(
                        session_handle_str, backend=backend.backend_id
                    )
                    await backend.delete_session_folder(session_handle)
                except Exception as exc:
                    logger.warning(
                        "Failed to delete main-cloud session folder for thread %s: %s",
                        thread_id,
                        exc,
                    )

    if not stateless:
        if (
            not force
            and expected_runtime_generation is None
            and not (
                thread.get("runtime_retirement_token") is not None
                and thread.get("runtime_retirement_authorized_at") is not None
            )
            and await _thread_turn_in_flight(thread)
        ):
            # Fast observational no-op for the common non-force refusal.  The
            # locked post-Begin probe below still catches a turn admitted in
            # the narrow interval before the token closes durable admission.
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "turn_in_flight",
                    "message": (
                        "The agent is mid-turn on this session. Retry with "
                        "force=true to end it anyway."
                    ),
                },
            )
        if expected_runtime_generation is not None:
            pre_begin = await postgres_db.get_thread(thread_id)
            identity_mismatch = bool(
                pre_begin is None
                or str(pre_begin.get("runtime_generation") or "")
                != expected_runtime_generation
                or str(pre_begin.get("agent_id") or "") != str(expected_agent_id or "")
                or str(pre_begin.get("runtime_attach_token") or "")
                != str(expected_attach_token or "")
            )
            if (
                not identity_mismatch
                and pre_begin is not None
                and pre_begin.get("runtime_retirement_token") is not None
            ):
                pending_context = pre_begin.get("runtime_retirement_context") or {}
                if isinstance(pending_context, str):
                    try:
                        pending_context = json.loads(pending_context)
                    except (TypeError, ValueError):
                        pending_context = {}
                identity_mismatch = bool(
                    not isinstance(pending_context, Mapping)
                    or str(pending_context.get("generation") or "")
                    != expected_runtime_generation
                    or str(pending_context.get("agent_id") or "")
                    != str(expected_agent_id or "")
                    or str(pending_context.get("runtime_attach_token") or "")
                    != str(expected_attach_token or "")
                    or bool(pre_begin.get("runtime_retirement_permanent"))
                    != bool(permanent)
                    or str(pending_context.get("settle_status") or "") != settle_status
                )
            if identity_mismatch:
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_runtime_identity_mismatch"},
                )
        begin_identity: dict[str, Any] = {}
        if expected_runtime_generation is not None:
            begin_identity = {
                "expected_runtime_generation": expected_runtime_generation,
                "expected_agent_id": expected_agent_id,
                "expected_attach_token": expected_attach_token,
                "require_agent_offline": require_expected_agent_offline,
            }
        retirement = await _begin_pinned_thread_retirement(
            thread_id,
            permanent=permanent,
            settle_status=settle_status,
            **begin_identity,
        )
        state = str(retirement.get("state") or "")
        if state == "missing":
            return {"status": "deleted"}
        if state == "settled" and not permanent:
            return {"status": settle_status}
        if state != "pending":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "pinned_retirement_conflict",
                    "reason": retirement.get("reason") or state,
                    **({"message": "Close active IDE or SSH access, then retry ending this session"}
                       if retirement.get("reason") == "active_workspace_access" else {}),
                },
            )
        # Idle admission installs this row atomically with the same pinned
        # retirement token. A watchdog, owner retry or another replica must
        # recover the physical-Pod-stop requirement from durable authority,
        # never from the initiating caller's in-memory option.
        idle_stop = await postgres_db.fetchrow(
            "SELECT id FROM vm_idle_operations WHERE owner_kind='thread' "
            "AND owner_id=$1::uuid AND release_kind='pinned_thread' "
            "AND thread_runtime_generation=$2::uuid "
            "AND thread_retirement_token=$3::uuid AND closed_at IS NULL",
            thread_id, retirement["generation"], retirement["token"],
        )
        if require_physical_agent_stop and idle_stop is None:
            raise HTTPException(
                status_code=409,
                detail={"code": "pinned_idle_retirement_source_changed"},
            )
        require_physical_agent_stop = idle_stop is not None
        already_authorized = retirement.get("authorized_at") is not None

        async def _abort_hidden_preflight() -> str:
            """Resolve this request's pre-authorization marker exactly.

            ``aborted`` is the only outcome that permits a refusal to be
            reported as an observational no-op. A concurrent authorization is
            irrevocable and must converge as ``ending``; an exact hidden
            marker that somehow survives two CAS attempts is left for the
            bounded stale-preflight reaper and reported as recovery-required.
            """

            token = str(retirement["token"])
            generation = str(retirement["generation"])
            if await postgres_db.abort_pinned_thread_retirement(
                thread_id, token=token, generation=generation
            ):
                return "aborted"
            current = await postgres_db.get_thread(thread_id)
            if current is None:
                return "superseded"
            if (
                str(current.get("runtime_generation") or "") != generation
                or str(current.get("runtime_retirement_token") or "") != token
            ):
                return "superseded"
            if current.get("runtime_retirement_authorized_at") is not None:
                return "authorized"
            if await postgres_db.abort_pinned_thread_retirement(
                thread_id, token=token, generation=generation
            ):
                return "aborted"
            current = await postgres_db.get_thread(thread_id)
            if (
                current is None
                or str(current.get("runtime_generation") or "") != generation
                or str(current.get("runtime_retirement_token") or "") != token
            ):
                return "superseded"
            if current.get("runtime_retirement_authorized_at") is not None:
                return "authorized"
            return "preflight"

        def _ending_response(
            *,
            retry_after_ms: int | None = None,
            retiring_agent_exit_authorized: bool = False,
        ) -> dict[str, Any]:
            response: dict[str, Any] = {
                "status": "ending",
                "retirement_disposition": settle_status,
                "retirement_permanent": bool(permanent),
            }
            if retry_after_ms is not None:
                response["retry_after_ms"] = retry_after_ms
            if retiring_agent_exit_authorized:
                response["retiring_agent_exit_authorized"] = True
                response["session_runtime_retirement_token"] = str(retirement["token"])
            return response

        if expected_runtime_generation is not None:
            captured = retirement.get("context") or {}
            identity_matches = bool(
                str(retirement.get("generation") or "") == expected_runtime_generation
                and str(captured.get("agent_id") or "") == str(expected_agent_id or "")
                and str(captured.get("runtime_attach_token") or "")
                == str(expected_attach_token or "")
            )
            if not identity_matches:
                if not retirement.get("reused") and not already_authorized:
                    abort_outcome = await _abort_hidden_preflight()
                    if abort_outcome == "authorized":
                        return _ending_response(retry_after_ms=250)
                raise HTTPException(
                    status_code=409,
                    detail={"code": "pinned_runtime_identity_mismatch"},
                )

        # The protected-engage producer and every pinned workspace ensure use
        # this same cross-replica lock. Admission was closed *before* waiting,
        # so old work can finish/roll back but no replacement may start.
        async with postgres_db.try_thread_advisory_lock(thread_id) as lock_owner:
            if not lock_owner:
                if already_authorized:
                    return _ending_response(retry_after_ms=250)
                abort_outcome = await _abort_hidden_preflight()
                if abort_outcome == "authorized":
                    return _ending_response(retry_after_ms=250)
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": (
                            "pinned_retirement_preflight_busy"
                            if abort_outcome in {"aborted", "superseded"}
                            else "pinned_retirement_preflight_recovery_required"
                        ),
                        "message": (
                            "Runtime lifecycle work is still converging; retry End."
                        ),
                    },
                )
            if not await _pinned_retirement_is_current(retirement):
                raise HTTPException(
                    status_code=409, detail="Pinned retirement authority changed"
                )
            context = retirement.get("context")
            context = {} if context is None else context
            captured_agent = (
                context.get("agent") if isinstance(context, Mapping) else None
            )
            probe_thread = {
                "id": thread_id,
                "runtime_generation": retirement.get("generation"),
                "runtime_retirement_token": retirement.get("token"),
                "runtime_retirement_context": context,
                "agent_id": (captured_agent or {}).get("id")
                if isinstance(captured_agent, Mapping)
                else None,
            }
            if (
                not already_authorized
                and not force
                and await _thread_turn_in_flight(probe_thread)
            ):
                abort_outcome = await _abort_hidden_preflight()
                if abort_outcome == "authorized":
                    return _ending_response(retry_after_ms=250)
                if abort_outcome == "preflight":
                    raise HTTPException(
                        status_code=503,
                        detail={
                            "code": "pinned_retirement_preflight_recovery_required"
                        },
                    )
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "turn_in_flight",
                        "message": (
                            "The agent is mid-turn on this session. Retry with "
                            "force=true to end it anyway."
                        ),
                    },
                )

            authoritative_thread = await postgres_db.get_thread(thread_id)
            if authoritative_thread is None:
                return {"status": "deleted"}
            officer_handoff = None
            authoritative_metadata = thread_metadata_object(authoritative_thread)
            officer_cfg = (authoritative_metadata.get("config_override") or {}).get(
                "officer"
            ) or {}
            linked_officer_authorization = bool(
                not already_authorized
                and settle_status == "ended"
                and isinstance(officer_cfg, Mapping)
                and officer_cfg.get("enabled") in (True, "true", "True", 1)
                and authoritative_thread.get("project_id") is not None
            )

            # A linked Officer's no-force in-flight decision and its durable
            # post mutation share one transaction with authorization. Every
            # other runtime authorizes before any stand-down/conference
            # mutation. Thus an unauthorized marker remains safe for the
            # stale-preflight reaper to clear after a crashed request.
            if linked_officer_authorization:
                try:
                    officer_handoff = await _stand_down(
                        authoritative_thread,
                        retirement_authority=retirement,
                    )
                except BaseException:
                    abort_outcome = await asyncio.shield(_abort_hidden_preflight())
                    if abort_outcome == "authorized":
                        return _ending_response(retry_after_ms=250)
                    raise
                if officer_handoff and officer_handoff.get("blocked_by_in_flight"):
                    abort_outcome = await _abort_hidden_preflight()
                    if abort_outcome == "authorized":
                        return _ending_response(retry_after_ms=250)
                    if abort_outcome == "preflight":
                        raise HTTPException(
                            status_code=503,
                            detail={
                                "code": "pinned_retirement_preflight_recovery_required"
                            },
                        )
                    return officer_post_lifecycle_service.officer_in_flight_decommission_response(
                        officer_handoff
                    )
                already_authorized = True
                retirement["authorized_at"] = True
            elif not already_authorized:
                if not await postgres_db.authorize_pinned_thread_retirement(
                    thread_id,
                    token=str(retirement["token"]),
                    generation=str(retirement["generation"]),
                    settle_status=settle_status,
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={"code": "pinned_retirement_authorization_refused"},
                    )
                retirement["authorized_at"] = True
                already_authorized = True

            # Authorization is append-only. Retries must replay these
            # idempotent obligations instead of skipping them after a crash.
            if settle_status == "ended" and not linked_officer_authorization:
                await _stand_down(
                    authoritative_thread,
                    converge_authorized=already_authorized,
                )
            authoritative_thread = (
                await postgres_db.get_thread(thread_id) or authoritative_thread
            )

            runtime_exposed = _retirement_context_runtime_exposed(retirement)
            local_quiescence = _retirement_has_exact_local_quiescence(
                retirement, authoritative_thread
            )
            if (
                runtime_exposed
                and not local_quiescence
                and _agent_pod_provision_intent_zero_candidate(
                    retirement, authoritative_thread
                )
            ):
                # The provision intent predates the Pod create effect and no
                # session owner ever bound. Observe/delete only its exact
                # attempt-labelled name, then receipt the durable absence.
                recovered_provision_intent = (
                    await _recover_agent_pod_provision_intent_zero(
                        retirement, authoritative_thread
                    )
                )
                if recovered_provision_intent:
                    authoritative_thread = (
                        await postgres_db.get_thread(thread_id) or authoritative_thread
                    )
                    local_quiescence = _retirement_has_exact_local_quiescence(
                        retirement, authoritative_thread
                    )
            if (
                runtime_exposed
                and not local_quiescence
                and _pre_registration_agent_pod_zero_candidate(
                    retirement, authoritative_thread
                )
            ):
                # Begin closed registration before this lifecycle lock was
                # acquired. A complete pre-registration Pod has no session
                # identity that can ACK, so End owns its exact UID stop and
                # atomically clears only that captured marker with the proof.
                recovered_pre_registration = (
                    await _recover_pre_registration_agent_pod_zero(
                        retirement, authoritative_thread
                    )
                )
                if recovered_pre_registration:
                    authoritative_thread = (
                        await postgres_db.get_thread(thread_id) or authoritative_thread
                    )
                    local_quiescence = _retirement_has_exact_local_quiescence(
                        retirement, authoritative_thread
                    )
            prior_soft_settlement = False
            if permanent and runtime_exposed and not local_quiescence:
                prior_soft_settlement = (
                    await postgres_db.pinned_thread_has_prior_soft_settlement(
                        thread_id,
                        runtime_generation=str(retirement["generation"]),
                        retirement_token=str(retirement["token"]),
                    )
                )
            raw_local_quiescence = authoritative_thread.get(
                "runtime_retirement_local_quiescence"
            )
            if isinstance(raw_local_quiescence, str):
                try:
                    raw_local_quiescence = json.loads(raw_local_quiescence)
                except (TypeError, ValueError):
                    raw_local_quiescence = None
            agent_local_quiescence = bool(
                local_quiescence
                and isinstance(raw_local_quiescence, Mapping)
                and raw_local_quiescence.get("quiescence_actor") == "agent"
            )
            context = retirement.get("context")
            context = {} if context is None else context
            captured_agent = (
                context.get("agent") if isinstance(context, Mapping) else None
            )
            captured_agent_pod = (
                context.get("agent_pod") if isinstance(context, Mapping) else None
            )
            captured_actor_shapes_valid = bool(
                all(
                    value is None or isinstance(value, Mapping)
                    for value in (captured_agent, captured_agent_pod)
                )
                and all(
                    not value
                    or (
                        str(value.get(name_key) or "")
                        and str(value.get("pod_uid") or "")
                    )
                    for value, name_key in (
                        (captured_agent, "hostname"),
                        (captured_agent_pod, "pod_name"),
                    )
                    if isinstance(value, Mapping)
                )
            )
            orchestrator_destructive_zero = bool(
                permanent
                and runtime_exposed
                and not local_quiescence
                and not prior_soft_settlement
                and isinstance(context, Mapping)
                and captured_actor_shapes_valid
                and str(context.get("entry_status") or "") == "ended"
                and not context.get("agent_id")
                and not context.get("runtime_attach_token")
                and authoritative_thread.get("agent_id") is None
                and authoritative_thread.get("runtime_attach_token") is None
                and authoritative_thread.get("control_admission_agent_id") is None
                and str(authoritative_thread.get("status") or "") == "ended"
            )
            if runtime_exposed and not (
                local_quiescence
                or prior_soft_settlement
                or orchestrator_destructive_zero
            ):
                # Owner End, offline recovery and a lost response may close
                # admission, but Pod absence/offline status is not proof that
                # tmux/nohup writers in the workspace stopped.  Return the
                # durable in-progress state without staging, snapshotting,
                # revoking or deleting anything.  The exact agent watchdog
                # performs strict local cleanup, appends the acknowledgement,
                # then retries this same immutable retirement.
                if local_runtime_quiesced:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "pinned_local_quiescence_refused",
                            "message": (
                                "The exact local cleanup acknowledgement was "
                                "not persisted."
                            ),
                        },
                    )
                return {
                    "status": "ending",
                    "retirement_disposition": settle_status,
                    "retirement_permanent": bool(permanent),
                }

            if require_physical_agent_stop and local_runtime_quiesced:
                # The reporting agent's own HTTP response cannot wait for
                # exact deletion of its Pod. Its durable local-zero ACK is
                # now visible; the independent idle reconciler stops the
                # captured Pod and continues this same token after response.
                return _ending_response(retry_after_ms=250)

            try:
                staged_event: dict[str, Any] | None = None
                authoritative_metadata = thread_metadata_object(authoritative_thread)
                # A permanent delete intentionally discards both the runtime
                # and any pending review.  Do not spend minutes materialising
                # a potentially multi-gigabyte overlay only to delete its
                # thread/event rows immediately afterwards, and do not let an
                # unavailable staging backend wedge an otherwise-authorised
                # permanent deletion.  Soft End/Suspend must finish and
                # durably announce staging before settlement reopens Resume.
                if not permanent and authoritative_metadata.get("protected_cloud"):
                    from orchestrator.services.cloud_staging.stage import (
                        stage_thread_cloud_diff,
                        staging_manifest_key,
                    )

                    ro_row = await postgres_db.get_ro_mount_by_thread(thread_id)
                    receipt_present = (
                        authoritative_thread.get("runtime_retirement_stage_receipt")
                        is not None
                    )
                    receipt_valid, receipt_event = _retirement_stage_event_from_receipt(
                        retirement, authoritative_thread, ro_row
                    )
                    if receipt_present and not receipt_valid:
                        raise RuntimeError(
                            "protected retirement stage receipt is inconsistent"
                        )
                    if receipt_valid:
                        # A previous attempt crossed the sole visibility CAS;
                        # workspace/reader cleanup may already have happened.
                        # Reuse the exact receipt and perform no SSH or S3 IO.
                        staged_event = receipt_event
                    else:
                        stage_authority = _capture_cloud_stage_authority(
                            authoritative_thread, ro_row or {}
                        )
                        if stage_authority is None:
                            captured_ro = (retirement.get("context") or {}).get(
                                "protected_ro"
                            )
                            captured_summary = (
                                captured_ro.get("staged_summary")
                                if isinstance(captured_ro, Mapping)
                                else None
                            )
                            manifest_present = bool(
                                isinstance(captured_summary, dict)
                                and await snapshot_service.get_blob(
                                    staging_manifest_key(thread_id, captured_summary)
                                )
                                is not None
                            )
                            pre_staged = None
                            if manifest_present:
                                pre_staged = await postgres_db.publish_quiesced_retirement_existing_stage_receipt(
                                    thread_id,
                                    expected_runtime_generation=str(
                                        retirement["generation"]
                                    ),
                                    expected_retirement_token=str(retirement["token"]),
                                )
                            if pre_staged is None:
                                # A protected create may be ended before workspace
                                # attach or credential delivery.  An engaging (or
                                # fully probed active) reader grant is an external
                                # resource, not process exposure: exact-revoke its
                                # captured attempt before publishing the distinct
                                # zero-stage receipt.  This deliberately does not
                                # run the rest of cleanup before mandatory staging.
                                await _revoke_never_delivered_protected_reader(
                                    retirement,
                                    authoritative_thread,
                                    ro_row,
                                )
                                never_engaged = await postgres_db.publish_never_engaged_retirement_stage_receipt(
                                    thread_id,
                                    expected_runtime_generation=str(
                                        retirement["generation"]
                                    ),
                                    expected_retirement_token=str(retirement["token"]),
                                )
                                if never_engaged is None:
                                    raise RuntimeError(
                                        "protected retirement lacks exact staging authority"
                                    )
                            authoritative_thread = (
                                await postgres_db.get_thread(thread_id)
                                or authoritative_thread
                            )
                            receipt_valid, receipt_event = (
                                _retirement_stage_event_from_receipt(
                                    retirement,
                                    authoritative_thread,
                                    await postgres_db.get_ro_mount_by_thread(thread_id),
                                )
                            )
                            if not receipt_valid:
                                raise RuntimeError(
                                    (
                                        "pre-staged retirement receipt is inconsistent"
                                        if pre_staged is not None
                                        else "never-engaged retirement receipt is inconsistent"
                                    )
                                )
                            staged_event = receipt_event
                        else:
                            if str(
                                stage_authority.get("runtime_retirement_token") or ""
                            ) != str(retirement["token"]):
                                raise RuntimeError(
                                    "protected retirement lacks exact staging authority"
                                )
                            stage_result = await stage_thread_cloud_diff(
                                thread_id=thread_id,
                                postgres_db=postgres_db,
                                snapshot_service=snapshot_service,
                                authority=stage_authority,
                                vm_provisioner=vm_provisioner,
                            )
                            if stage_result is None or stage_result.get("skipped") in {
                                "authority_changed",
                                "no_active_mount",
                                "no_workspace",
                            }:
                                raise RuntimeError(
                                    "protected retirement staging remains retryable"
                                )
                            publication = stage_result.get("publication") or {}
                            if not isinstance(
                                publication.get("retirement_stage_receipt"), dict
                            ):
                                raise RuntimeError(
                                    "protected retirement staging has no durable receipt"
                                )
                            if isinstance(stage_result.get("event"), dict):
                                staged_event = dict(stage_result["event"])
                raw_agent_workspace_claim = (
                    context.get("agent_workspace_claim")
                    if isinstance(context, Mapping)
                    else None
                )
                agent_workspace_exit_handoff = bool(
                    permanent
                    and retiring_agent_response_pending
                    and runtime_exposed
                    and local_quiescence
                    and raw_agent_workspace_claim not in (None, {})
                )
                await _cleanup_pinned_thread_retirement(
                    retirement,
                    stop_agent_before_workspace=require_physical_agent_stop,
                    cleanup_agent_pod=(
                        require_physical_agent_stop
                        or not runtime_exposed
                        or (
                            permanent
                            and not retiring_agent_response_pending
                            and (
                                agent_local_quiescence
                                or prior_soft_settlement
                                or orchestrator_destructive_zero
                            )
                        )
                    ),
                    defer_agent_workspace_claim_until_caller_exit=(
                        agent_workspace_exit_handoff
                    ),
                )
                if agent_workspace_exit_handoff:
                    return _ending_response(retiring_agent_exit_authorized=True)
                if permanent:
                    await _delete_auxiliary_state(authoritative_thread)
                    await postgres_db.delete_thread(
                        thread_id,
                        expected_runtime_retirement_token=str(retirement["token"]),
                        expected_runtime_generation=str(retirement["generation"]),
                    )
                else:
                    settled = await postgres_db.settle_pinned_thread_retirement(
                        thread_id,
                        token=str(retirement["token"]),
                        generation=str(retirement["generation"]),
                        final_status=settle_status,
                        staged_event=staged_event,
                    )
                    if not settled:
                        raise RuntimeError("Pinned retirement settlement CAS failed")
                    if not await _complete_retiring_soft_warm_binding_release(
                        retirement
                    ):
                        # Settlement already moved the exact receipt to
                        # ``releasing``. The leader can retry that durable
                        # finalizer obligation, but this request must not claim
                        # the warm pool slot is available yet.
                        raise RuntimeError(
                            "Pinned retirement warm release remains retryable"
                        )
            except HTTPException:
                raise
            except Exception as exc:
                logger.exception(
                    "Pinned retirement remains retryable for thread %s", thread_id
                )
                raise HTTPException(
                    status_code=503,
                    detail={
                        "code": "pinned_retirement_retry_pending",
                        "message": "Runtime cleanup is incomplete; retry End.",
                    },
                ) from exc

        result: dict[str, Any] = {"status": "deleted" if permanent else settle_status}
        if include_officer_handoff:
            result["_officer_handoff"] = officer_handoff
        return result

    # Every stateless tier uses the queue lifecycle, including lite sessions.
    # The validator keeps VM/unknown tiers refused while admitting only the
    # narrowed sandbox and lite authority shapes.
    _require_stateless_end_workspace(thread)
    initial_stateless_authority = (
        await postgres_db.get_stateless_thread_lifecycle_authority(thread_id)
    )
    if initial_stateless_authority is None:
        raise HTTPException(status_code=409, detail="Thread lifecycle changed")
    try:
        async with postgres_db.stateless_session_workspace_ensure_lock(
            thread_id, wait=True
        ) as cleanup_owner:
            if not cleanup_owner:
                raise HTTPException(
                    status_code=503,
                    detail="Stateless workspace lifecycle lock unavailable",
                )
            fresh_thread = await postgres_db.get_thread(thread_id)
            if fresh_thread is None:
                return {"status": "deleted"}
            _require_stateless_end_workspace(fresh_thread)
            fresh_authority = (
                await postgres_db.get_stateless_thread_lifecycle_authority(thread_id)
            )
            if fresh_authority != initial_stateless_authority:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Thread lifecycle generation changed while waiting for "
                        "cleanup ownership"
                    ),
                )
            fresh_metadata = thread_metadata_object(fresh_thread)
            marker_pending = bool(
                "_stateless_workspace_retirement_pending" in fresh_metadata
            )
            fresh_status = fresh_thread.get("status")
            if (
                fresh_status != initial_status
                and not (permanent and fresh_status == "ended")
                and not marker_pending
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Thread lifecycle changed while waiting for cleanup ownership"
                    ),
                )

            result = await _reconcile_stateless_thread_retirement(
                thread_id,
                force=force,
                permanent=permanent,
            )
            if result.get("state") == "missing":
                return {"status": "deleted"}

            # These side effects are serialized after the fresh-state check;
            # a stale End can no longer mutate a thread that End->Resume reopened.
            await _stand_down(fresh_thread)
            if permanent:
                closure = result.get("closure") or {}
                terminal_thread = result.get("thread") or fresh_thread
                # The final transcript is the source of a separately leased
                # session-turn memory obligation. Retirement may remove the
                # runtime workspace, but permanent deletion must retain the
                # DB transcript/config until that obligation is terminal.
                # ``delete_thread`` repeats this under its row locks.
                if await postgres_db.has_unfinished_session_memory_effects(thread_id):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Final-memory extraction is still pending; retry "
                            "permanent deletion after it settles"
                        ),
                    )
                terminal_metadata = thread_metadata_object(terminal_thread)
                terminal_binding = terminal_metadata.get("_workspace_binding") or {}
                terminal_workspace = terminal_metadata.get("workspace_container") or {}
                terminal_backing_id = (
                    closure.get("backing_id")
                    if isinstance(closure, dict) and closure.get("backing_id")
                    else (
                        terminal_binding.get("backing_id")
                        if isinstance(terminal_binding, dict)
                        else None
                    )
                )
                terminal_snapshot_exists = bool(
                    isinstance(closure, dict)
                    and (
                        closure.get("snapshot_restore_required") is True
                        or (
                            isinstance(terminal_workspace, dict)
                            and terminal_workspace.get("_snapshot_restore_required")
                            is True
                        )
                        or (str(terminal_backing_id or "").startswith("k8s-pod:"))
                    )
                )
                if terminal_snapshot_exists and not snapshot_service.is_available:
                    raise HTTPException(
                        status_code=503,
                        detail="Terminal workspace snapshot cleanup is unavailable",
                    )
                if (
                    snapshot_service.is_available
                    and not await snapshot_service.delete_snapshot(
                        thread_id,
                        entity_type="threads",
                    )
                ):
                    # The thread/queue tombstone remains retryable. Never
                    # delete the only ownership row while its terminal S3
                    # prefix may survive indefinitely.
                    raise HTTPException(
                        status_code=503,
                        detail="Terminal workspace snapshot cleanup is incomplete",
                    )
                if declared_thread_workspace_backend(terminal_thread) == "virtual":
                    from orchestrator.services.thread_uploads import (
                        purge_attested_stateless_virtual_workspace,
                    )

                    if not await purge_attested_stateless_virtual_workspace(
                        terminal_thread
                    ):
                        raise HTTPException(
                            status_code=503,
                            detail=("Terminal virtual workspace cleanup is incomplete"),
                        )
                from orchestrator.services.stateless_workspace_history_cleanup import (
                    reclaim_stateless_workspace_history,
                )

                if not await reclaim_stateless_workspace_history(
                    postgres_db, container_provisioner, thread_id
                ):
                    raise HTTPException(
                        status_code=503,
                        detail="Historical workspace permanent cleanup is incomplete",
                    )
                await _delete_auxiliary_state(fresh_thread)
                await postgres_db.delete_thread(thread_id)
                return {"status": "deleted"}
            return {"status": "ended"}
    except asyncio.TimeoutError as exc:
        raise HTTPException(
            status_code=503,
            detail="Stateless workspace lifecycle lock acquisition timed out",
        ) from exc


async def archive_and_cleanup_workspace(
    entity_id: str,
    entity_type: str = "jobs",
    *,
    reclaim_volume: bool = True,
    dependencies: ThreadRetirementDependencies,
) -> list[str]:
    """Snapshot workspace to S3, then delete container/VM.

    Centralized cleanup for all workspace teardown paths (job completion,
    cancellation, cascade cleanup, thread end). Each provisioner's release
    method handles snapshot-before-delete internally.

    Args:
        entity_id: Job or thread UUID.
        entity_type: "jobs" or "threads".
        reclaim_volume: Threads only — whether the workspace PVC dies with the
            pod. The caller owns this decision because "release a thread's
            workspace" covers two different intents: a thread whose status is
            ``ended`` is still RESUMABLE (``resume_thread`` requires exactly
            that status, and the agent's idle-archive sets it automatically
            after 30 idle minutes), so tearing its volume down would silently
            destroy a workspace the user can still reopen. Only a genuine
            permanent delete passes True. Jobs keep the default: reclaiming a
            job's PVC on a terminal state is correct and unchanged.

    Returns:
        List of action descriptions for logging.
    """

    postgres_db = dependencies.store
    container_provisioner = dependencies.container_provisioner
    vm_provisioner = dependencies.vm_provisioner
    recovery_store = dependencies.recovery_store
    docker_provisioner = dependencies.docker_provisioner
    _get_container_context = dependencies.get_container_context
    _get_vm_context = dependencies.get_vm_context
    _vm_needs_release = dependencies.vm_needs_release

    actions: list[str] = []

    if entity_type == "threads":
        thread = await postgres_db.get_thread(entity_id)
        if not thread:
            return actions
        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        ws_ctx = metadata.get("workspace_container") or {}
        vm_ctx = metadata.get("vm") or {}

        # Workspace container cleanup (snapshot + delete)
        if ws_ctx.get("status") not in ("deleted", "deleting", "released", None):
            if ws_ctx.get("provisioner") == "docker":
                released = await docker_provisioner.release_thread_workspace(entity_id)
                if not released:
                    raise RuntimeError(
                        "Docker thread workspace authority retirement is incomplete"
                    )
                actions.append("docker thread workspace authority retired")
            elif container_provisioner.is_available:
                # reclaim_volume=False snapshots + deletes the pod but KEEPS the
                # PVC, so a resumable `ended` thread comes back to its own files
                # instead of an empty volume (see the docstring above).
                teardown_identity = (
                    await container_provisioner.capture_terminal_workspace_identity(
                        WorkspaceOwner.session(entity_id)
                    )
                )
                released = await container_provisioner.release_workspace(
                    WorkspaceOwner.session(entity_id),
                    reclaim_volume=reclaim_volume,
                    teardown_identity=teardown_identity,
                    strict=True,
                )
                if not released:
                    raise RuntimeError(
                        "Kubernetes thread workspace exact teardown is incomplete"
                    )
                actions.append(
                    "k8s thread workspace released"
                    + ("" if reclaim_volume else " (volume kept)")
                )

        # VM cleanup (snapshot + delete)
        if _vm_needs_release(vm_ctx):
            if vm_provisioner.lifecycle_available:
                teardown_identity = await vm_provisioner.capture_vm_teardown_identity(
                    entity_id,
                    entity_type="thread",
                )
                cleanup = await acquire_vm_cleanup_permit(
                    recovery_store,
                    owner_kind="thread",
                    owner_id=entity_id,
                    identity=teardown_identity,
                    source="thread_terminal_vm_release",
                    purge_disk=True,
                )
                if not cleanup.allowed:
                    raise RuntimeError("thread VM cleanup held for workspace recovery")
                disposition = completed_cleanup_outcome(cleanup)
                if disposition is None:
                    outcome = await vm_provisioner.release_vm_captured(
                        entity_id,
                        teardown_identity,
                        ssh_host=vm_ctx.get("ssh_host"),
                        ssh_port=vm_ctx.get("ssh_port"),
                        entity_type="thread",
                        purge_disk=True,
                        **vm_cleanup_kwargs(cleanup),
                    )
                    disposition = outcome.disposition
                    if disposition in {"completed", "identity_superseded"}:
                        await complete_vm_cleanup_permit(
                            recovery_store,
                            cleanup,
                            outcome=disposition,
                        )
                if disposition != "completed":
                    raise RuntimeError(
                        "Thread VM exact teardown remains " + str(disposition)
                    )
                actions.append("thread vm released")

    else:
        job = await postgres_db.get_job(entity_id)
        if not job:
            return actions
        raw_context = job.get("context") or {}
        if isinstance(raw_context, str):
            try:
                raw_context = json.loads(raw_context)
            except (TypeError, ValueError):
                raw_context = {}
        if (
            job.get("parent_job_id")
            and isinstance(raw_context, dict)
            and raw_context.get("inherits_parent_workspace") is True
        ):
            # Inherited subjobs carry a diagnostic copy of the root runtime,
            # but the stable Pod/VM/Docker owner remains the root job. A child
            # terminal transition owns only its worktree/claim—not namespace-
            # wide credential retirement or compute deletion.
            actions.append("inherited workspace retained by root owner")
            return actions
        ws_ctx = _get_container_context(job)
        vm_ctx = _get_vm_context(job)

        # A controller may have stopped the VM and marked its context deleting
        # before the parent admission/compute charge committed. Replay that
        # same exact terminal admission until authenticated absence settles it.
        pending_terminal_vm_cleanup = False
        if vm_ctx and not _vm_needs_release(vm_ctx):
            async with postgres_db.acquire() as conn:
                pending_terminal_vm_cleanup = bool(await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
                    "WHERE owner_kind='job' AND owner_id=$1::uuid "
                    "AND source='job_terminal_vm_release' "
                    "AND pvc_uid::text IS NOT DISTINCT FROM $2::text "
                    "AND completed_at IS NULL)",
                    entity_id, vm_ctx.get("rootdisk_pvc_uid"),
                ))

        # VM cleanup (snapshot + delete)
        if _vm_needs_release(vm_ctx) or pending_terminal_vm_cleanup:
            if vm_provisioner.lifecycle_available:
                teardown_identity = await vm_provisioner.capture_vm_teardown_identity(
                    entity_id
                )
                purge_disk = True
                if vm_ctx.get("workspace_storage") is not None:
                    # The context is a hint; only the durable reservation can
                    # authorize keeping this rootdisk after terminal compute.
                    if await vm_provisioner._storage_context(entity_id) is None:
                        raise RuntimeError("VM retained storage authority is unavailable")
                    purge_disk = False
                from orchestrator.services.vm_idle_lifecycle import (
                    retained_terminal_rootdisk,
                )

                if await retained_terminal_rootdisk(
                    postgres_db, job_id=entity_id,
                    generation=teardown_identity.provision_generation,
                    pvc_uid=teardown_identity.rootdisk_pvc_uid,
                ):
                    raise RuntimeError("terminal VM rootdisk belongs to idle review release")
                cleanup = await acquire_vm_cleanup_permit(
                    recovery_store,
                    owner_kind="job",
                    owner_id=entity_id,
                    identity=teardown_identity,
                    source="job_terminal_vm_release",
                    purge_disk=purge_disk,
                )
                if not cleanup.allowed:
                    raise RuntimeError("job VM cleanup held for workspace recovery")
                marker = (
                    raw_context.get("_job_terminal_vm_cleanup")
                    if isinstance(raw_context, dict) else None
                )
                if marker is not None and not await postgres_db.bind_terminal_vm_cleanup_admission(
                    entity_id,
                    expected_generation=teardown_identity.provision_generation,
                    admission_id=cleanup.admission_id,
                    pvc_uid=teardown_identity.rootdisk_pvc_uid,
                ):
                    raise RuntimeError("terminal Job VM cleanup admission changed")
                disposition = completed_cleanup_outcome(cleanup)
                if disposition is None:
                    outcome = await vm_provisioner.release_vm_captured(
                        entity_id,
                        teardown_identity,
                        ssh_host=vm_ctx.get("ssh_host"),
                        ssh_port=vm_ctx.get("ssh_port"),
                        purge_disk=purge_disk,
                        **vm_cleanup_kwargs(cleanup),
                    )
                    disposition = outcome.disposition
                    if disposition in {"completed", "identity_superseded"}:
                        await complete_vm_cleanup_permit(
                            recovery_store,
                            cleanup,
                            outcome=disposition,
                            provisioner=vm_provisioner,
                        )
                if disposition != "completed":
                    raise RuntimeError("VM exact teardown remains " + str(disposition))
                actions.append("vm released")

        # Workspace container cleanup (snapshot + delete)
        if ws_ctx and ws_ctx.get("status") not in (
            "deleted",
            "deleting",
            "released",
            None,
        ):
            if ws_ctx.get("provisioner") == "docker":
                released = await docker_provisioner.release_workspace(entity_id)
                if not released:
                    raise RuntimeError(
                        "Docker workspace authority retirement is incomplete"
                    )
                actions.append("docker workspace authority retired")
            else:
                owner = WorkspaceOwner.job(entity_id)
                runtime = ws_ctx.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
                replay = (
                    await container_provisioner.replay_terminal_workspace_cleanup(
                        owner, expected_runtime_incarnation=str(runtime)
                    )
                    if runtime is not None
                    else None
                )
                if replay is not None:
                    released = replay.settled
                else:
                    teardown_identity = (
                        await container_provisioner.capture_terminal_workspace_identity(
                            owner
                        )
                    )
                    released = await container_provisioner.release_workspace(
                        owner,
                        teardown_identity=teardown_identity,
                        strict=True,
                    )
                if not released:
                    raise RuntimeError(
                        "Kubernetes workspace exact teardown is incomplete"
                    )
                actions.append("k8s workspace released")

    return actions


async def detach_agent_session(
    thread_id: str,
    timeout: float = 150.0,
    *,
    dependencies: ThreadRetirementDependencies,
) -> bool:
    """Ask the thread's live agent to terminate its session, and wait.

    Gives the agent the chance to run its full terminate path — final
    memory capture (memory_bugs.md B11) and the workspace git push —
    BEFORE ``_release_thread_resources`` tears down the workspace and
    pod. Without this, the user-facing DELETE deleted the pod outright
    and the session's final extraction died with it (the agent kept
    heartbeating through the grace period, then got SIGKILLed).

    Best-effort by design: returns False (and never raises) when the
    thread has no bound agent, the agent isn't serving a session, or the
    call fails — teardown then proceeds exactly as before. The read
    timeout is sized to the persistent auxiliary extraction budget
    (auxiliary.timeout=120s) plus git-push headroom; unreachable pods
    fail in seconds via the connect timeout, and an already-terminated
    agent answers "already_idle" instantly.
    """

    postgres_db = dependencies.store
    logger = dependencies.logger
    _thread_uses_pinned_execution = dependencies.thread_uses_pinned_execution

    try:
        thread = await postgres_db.get_thread(thread_id)
        runtime_authority = thread_runtime_authority(thread)
        if runtime_authority is None or not _thread_uses_pinned_execution(thread):
            return False
        binding = await postgres_db.get_pinned_session_binding(
            thread_id,
            expected_runtime_generation=runtime_authority.generation,
        )
        # 'session' is the heartbeat status of an agent serving a live
        # session — anything else (ready/offline/busy) either has nothing
        # to capture or is unreachable, so skip fast and let the normal
        # teardown run.
        if binding is None or binding.agent_status != "session":
            return False
        url = f"http://{binding.pod_ip}:{binding.pod_port}/session/detach"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=3.0)
        ) as client:
            resp = await client.post(
                url,
                json={
                    "session_identity_fingerprint": (
                        binding.session_identity_fingerprint
                    )
                },
            )
        if resp.status_code == 200:
            logger.info(
                "Thread %s: agent detach completed before teardown",
                thread_id,
            )
            return True
        logger.warning(
            "Thread %s: pre-teardown detach returned %s — proceeding",
            thread_id,
            resp.status_code,
        )
        return False
    except Exception as e:
        logger.warning(
            "Thread %s: pre-teardown detach failed (%s: %s) — proceeding",
            thread_id,
            type(e).__name__,
            e,
        )
        return False


async def release_thread_resources(
    thread_id: str,
    *,
    reclaim_volume: bool = False,
    require_workspace_retirement: bool = False,
    dependencies: ThreadRetirementDependencies,
) -> None:
    """Release a thread's workspace container/VM and agent pod.

    Centralized so the user-facing DELETE, the agent-facing status flip,
    and the orphan reaper share one teardown sequence. Each step swallows
    its own exception — a failure in snapshotting must not block the
    agent-pod delete and vice versa, otherwise resources leak.

    ``reclaim_volume`` decides whether the workspace PVC dies with the pod, and
    defaults to False because most callers here do NOT mean "destroy this
    user's data". A thread reaching ``ended`` is resumable — ``resume_thread``
    accepts exactly that status, the idle-archive writes it after 30 idle
    minutes, and the orphan sweeper writes it whenever an agent pod merely goes
    offline. With PVC-backed session workspaces, reclaiming on those paths would
    permanently delete a workspace the user can still reopen, so only the
    permanent-delete branch of ``end_thread`` passes True. The safe default also
    means a future caller that forgets the argument leaks a volume (recoverable)
    rather than destroying one (not).
    """

    agent_provisioner = dependencies.agent_provisioner
    persistent_provisioner = dependencies.persistent_provisioner
    logger = dependencies.logger
    _detach_agent_session = partial(detach_agent_session, dependencies=dependencies)
    _archive_and_cleanup_workspace = partial(
        archive_and_cleanup_workspace, dependencies=dependencies
    )

    # Let a live session agent terminate cleanly (final memory capture +
    # git push) while its workspace still exists. No-op in seconds for
    # agent-initiated endings (already terminated → "already_idle") and
    # for the orphan reaper (agent not in 'session' status).
    await _detach_agent_session(thread_id)

    workspace_error: Exception | None = None
    try:
        await _archive_and_cleanup_workspace(
            thread_id, entity_type="threads", reclaim_volume=reclaim_volume
        )
    except Exception as exc:
        workspace_error = exc
        logger.exception("Workspace cleanup failed for thread %s", thread_id)

    # BOTH provisioners, not either/or: a thread can have pods from both over
    # its lifetime — pool pods (agent_provisioner) from normal attach, and a
    # dedicated persistent-<tid> pod + PVC from the officer watchdog's respawn
    # path. The old elif skipped the persistent cleanup whenever the pool
    # provisioner was available, leaking a 10Gi PVC per respawned officer on
    # retirement (k3d smoke, open item 7). Both deletes are idempotent no-ops
    # when nothing matches.
    try:
        if agent_provisioner.is_available:
            await agent_provisioner.delete_agent_pod_by_thread(thread_id)
    except Exception:
        logger.exception("Agent pod cleanup failed for thread %s", thread_id)
    try:
        if persistent_provisioner.is_available:
            await persistent_provisioner.delete_agent_pod(thread_id)
            # The pod is stateless and always goes; its PVC is the workspace
            # itself, so it follows the same rule as the container workspace
            # above — an `ended` thread is RESUMABLE (idle-archive and the
            # orphan sweeper both write that status without any user intent to
            # destroy data), and only a permanent delete reclaims the volume.
            # Deleting it unconditionally here meant an idle timeout or a mere
            # agent-pod crash wiped the workspace of any thread served by this
            # provisioner (magic-link wake, officer-watchdog respawn). The cost
            # is that the retirement leak noted above now drains on the
            # permanent delete rather than on every end — the right way round:
            # a leaked volume is recoverable, a deleted one is not.
            if reclaim_volume:
                await persistent_provisioner.delete_agent_pvc(thread_id)
    except Exception:
        logger.exception("Persistent pod cleanup failed for thread %s", thread_id)

    # The supported End path must not report a terminal transition while a
    # static workspace can still hold a live repo-scoped ssh-agent, or while a
    # Pod/VM delete has not proved the captured compute incarnation gone.  The
    # background orphan path retains its historical best-effort behavior so a
    # failed snapshot cannot block unrelated agent-pod cleanup; its error stays
    # visible for lifecycle reconciliation.
    if require_workspace_retirement and workspace_error is not None:
        raise workspace_error


async def suspend_thread_resources(
    thread_id: str,
    *,
    dependencies: ThreadRetirementDependencies,
) -> None:
    """Suspend a thread's workspace to S3 and release the agent pod.

    Used for agent-initiated `ended` transitions where the user has not
    asked to destroy data — idle timeout, drain, watchdog, WS disconnect.
    Preserves the workspace via S3 snapshot so /resume can restore it
    later (resume already routes through restore_thread_workspace when
    it sees workspace_container.status == 'suspended').

    Falls back gracefully if the suspension service is disabled or the
    snapshot fails: the workspace stays alive (reconciler will reap it
    eventually) but we still delete the agent pod so the slot frees.
    """

    _threads_suspending = dependencies.threads_suspending
    logger = dependencies.logger
    _suspend_thread_resources_inner = partial(
        suspend_thread_resources_inner, dependencies=dependencies
    )

    if thread_id in _threads_suspending:
        logger.info(
            "Thread %s: suspend already in flight — skipping duplicate", thread_id
        )
        return
    _threads_suspending.add(thread_id)
    try:
        await _suspend_thread_resources_inner(thread_id)
    finally:
        _threads_suspending.discard(thread_id)


async def suspend_thread_resources_inner(
    thread_id: str,
    *,
    dependencies: ThreadRetirementDependencies,
) -> None:
    postgres_db = dependencies.store
    agent_provisioner = dependencies.agent_provisioner
    persistent_provisioner = dependencies.persistent_provisioner
    docker_provisioner = dependencies.docker_provisioner
    workspace_suspension_service = dependencies.workspace_suspension_service
    logger = dependencies.logger

    thread = await postgres_db.get_thread(thread_id)

    if thread is not None and thread.get("execution_lane") == "stateless":
        # Queue-served physical sessions require the terminal claimant /
        # resident / runtime protocol. Legacy suspension must not fall through
        # to name-only agent deletion when the central service refuses it.
        logger.info(
            "Skipping legacy resource suspension for stateless thread %s",
            thread_id,
        )
        return
    suspended = False
    metadata = thread_metadata_object(thread or {})
    workspace = metadata.get("workspace_container") or {}
    if workspace.get("provisioner") == "docker":
        # Static Docker suspension cannot destroy the process namespace.  It
        # must nevertheless retire and independently prove zero repo-scoped
        # ssh-agents before the persistent runtime Pod is removed.  The deploy
        # key remains server-side and can be delivered again on a supported
        # resume; only this resident bearer process is retired.
        try:
            if not await docker_provisioner.release_thread_workspace(thread_id):
                logger.error(
                    "Docker workspace authority retirement failed for ended thread %s",
                    thread_id,
                )
        except Exception:
            logger.exception(
                "Docker workspace authority retirement raised for ended thread %s",
                thread_id,
            )
    else:
        try:
            if workspace_suspension_service.is_enabled:
                suspended = await workspace_suspension_service.suspend_thread_workspace(
                    thread_id
                )
        except Exception:
            logger.exception("Workspace suspend failed for thread %s", thread_id)

    if suspended:
        # suspend_thread_workspace already deletes the agent pod.
        return

    logger.warning(
        "Workspace suspend unavailable or failed for thread %s — keeping "
        "workspace alive (reconciler will reap) but deleting the agent pod",
        thread_id,
    )
    # Pods from BOTH provisioners (see _release_thread_resources), but NOT the
    # persistent PVC: suspend serves resumable threads, and for a dedicated
    # pod that PVC is the workspace itself. It is reclaimed at retirement by
    # the release path.
    try:
        if agent_provisioner.is_available:
            await agent_provisioner.delete_agent_pod_by_thread(thread_id)
    except Exception:
        logger.exception("Agent pod cleanup failed for thread %s", thread_id)
    try:
        if persistent_provisioner.is_available:
            await persistent_provisioner.delete_agent_pod(thread_id)
    except Exception:
        logger.exception("Persistent pod cleanup failed for thread %s", thread_id)


@dataclass(frozen=True, slots=True)
class ThreadRetirementOperations:
    """Bound operation set used by HTTP, Officer and reconciliation callers."""

    dependencies: ThreadRetirementDependencies

    async def archive_and_cleanup_workspace(
        self,
        entity_id: str,
        entity_type: str = "jobs",
        *,
        reclaim_volume: bool = True,
    ) -> list[str]:
        return await archive_and_cleanup_workspace(
            entity_id,
            entity_type,
            reclaim_volume=reclaim_volume,
            dependencies=self.dependencies,
        )

    async def release_thread_resources(
        self,
        thread_id: str,
        *,
        reclaim_volume: bool = False,
        require_workspace_retirement: bool = False,
    ) -> None:
        await release_thread_resources(
            thread_id,
            reclaim_volume=reclaim_volume,
            require_workspace_retirement=require_workspace_retirement,
            dependencies=self.dependencies,
        )

    async def suspend_thread_resources(self, thread_id: str) -> None:
        await suspend_thread_resources(thread_id, dependencies=self.dependencies)

    def stateless_retirement_marker(self, thread: dict[str, Any]) -> dict[str, Any]:
        return stateless_retirement_marker(thread)

    async def thread_turn_in_flight(self, thread: dict[str, Any]) -> bool:
        return await thread_turn_in_flight(thread, dependencies=self.dependencies)

    async def reconcile_stateless_thread_retirement(
        self, thread_id: str, *, force: bool, permanent: bool
    ) -> dict[str, Any]:
        return await reconcile_stateless_thread_retirement(
            thread_id,
            force=force,
            permanent=permanent,
            dependencies=self.dependencies,
        )

    async def end_thread_flow(
        self,
        thread_id: str,
        thread: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        return await end_thread_flow(
            thread_id, thread, dependencies=self.dependencies, **kwargs
        )


__all__ = [
    "ThreadRetirementDependencies",
    "ThreadRetirementOperations",
    "archive_and_cleanup_workspace",
    "detach_agent_session",
    "end_thread_flow",
    "release_thread_resources",
    "reconcile_stateless_thread_retirement",
    "stateless_retirement_marker",
    "suspend_thread_resources",
    "suspend_thread_resources_inner",
    "thread_turn_in_flight",
]
