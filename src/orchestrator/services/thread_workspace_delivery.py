"""The cold-session workspace payload an agent receives for a thread.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). This is the
session half of B05's preparation scope, and it is deliberately *not* merged
with the worker half (``_build_job_start_request``): the two paths differ in
identity, fencing and payload, and a single function with a mode flag would
hide those differences rather than share them.

Four properties are load-bearing and moved unchanged:

* **The response is a credential-delivery boundary.** The caller holds the
  thread's datasource lock across the authoritative reads *and* the response
  build, so once an ``A -> B/[]`` selection save commits, no later cold
  response can still deliver the old ``A`` payload.
* **Pinned credentials are fenced to the reciprocal runtime owner.**
  :func:`require_pinned_workspace_credential_owner` refuses unless the
  presented agent, runtime generation and process attach identity match the
  stored authority — and a protected row never gets the mixed-version grace
  period, because its response carries the live lower-mount credential.
* **Attestation compares the live workspace against the stored binding, twice.**
  :func:`attest_pinned_thread_k8s_workspace` refuses a delivery whose runtime
  incarnation, backing id, host-key fingerprint, pod IP or port moved, and then
  re-reads the thread to refuse one whose authority moved *during* the attach.
* **A malformed provisioner authority is a 503, not a fallback.** Every refusal
  here fails closed.

Collaborators arrive through :class:`ThreadWorkspaceDeliveryDependencies`,
rebuilt per invocation by the application. Two of them — ``container_provisioner``
and ``gitea_client`` — are injected rather than imported specifically because
they are main-namespace singletons that tests rebind; importing them here would
resolve a different object than the one a caller patched.
"""

from __future__ import annotations

import asyncio  # noqa: F401  (used by the moved bodies)
import functools
import json
import logging
import os  # noqa: F401  (used by the moved bodies)
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional  # noqa: F401  (used by the moved bodies)
from uuid import UUID

from fastapi import HTTPException
from orchestrator.services.session_workspace_policy import preparation_wait_budget

from orchestrator.security.access import externalize_gitea_url
from orchestrator.security.access import require_internal as _require_internal
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
    WorkspaceRuntimeAttestation,
)
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    authorize_thread_repository_transport,
)
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
    thread_runtime_refusal_detail,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_thread_initial import (
    ensure_initial_thread_vm,
    initial_vm_wait_payload,
    is_initial_thread_vm_poll,
    require_current_initial_vm_source,
)
from orchestrator.services.workspace_binding import (
    remote_canvas_presentation_available,
    virtual_thread_backing_id,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from orchestrator.services.workspace_suspension import (
    WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY,
)
from shared.backend_kinds import LITE_BACKENDS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadWorkspaceDeliveryDependencies:
    """Collaborators for one cold-session payload build, per invocation.

    Every field is resolved by a main factory at call time rather than captured
    at import: ``store``, ``cloud_router``, ``gitea_client`` and
    ``container_provisioner`` are all rebound during ``lifespan`` or by tests,
    and the callables belong to other batches or other B05 lanes.

    The ``*_from_other_batch`` collaborators are named here rather than
    imported so this module never reaches across a boundary it does not own:
    B04 owns the cloud mount and protected-cloud operations, B06 owns thread
    runtime admission and project revalidation, and the remaining callables are
    B05's own policy and credential lanes.
    """

    store: Any
    cloud_router: Any
    gitea_client: Any
    container_provisioner: Any
    GrantDenied: Any
    LiteWorkspaceConfigError: Any
    backend_from_override: Any
    build_agent_cloud_mount: Any
    build_agent_cloud_sync: Any
    build_protected_cloud_mount: Any
    cloud_workspace_driver: Any
    grant_violations_detail: Any
    inject_lite_workspace_config: Any
    inject_thread_dispatch_credentials: Any
    protected_cloud_delivery_state: Any
    protected_workspace_wait_payload: Any
    require_pinned_status_identity: Any
    resolve_session_config: Any
    resolve_thread_datasources: Any
    resolve_thread_repositories: Any
    revalidate_thread_project_ids: Any
    ro_mount_matches_protected_selection: Any
    schedule_stateless_workspace_ensure: Any
    thread_accepts_runtime: Any
    thread_project_ids: Any
    thread_workspace_backend: Any
    virtual_workspace_rclone_spec: Any
    # Injected for the same reason as the callables above: it is patched on
    # ``orchestrator.main`` by an existing suite, and a direct import would
    # resolve past the patch.
    vm_workspaces_on_pod_network: Any
    # Same convention as `routers/agent_cloud_stage.py`: the internal-key
    # guard is a field with a default, so a router resolves it through the
    # dependency object and a caller can substitute it explicitly.
    require_internal: Any = _require_internal
    capture_session_config: Any = None
    vm_provisioner: Any = None


def agent_canvas_workspace_capabilities(
    metadata: dict[str, Any],
    workspace_context: dict[str, Any],
    vm_context: dict[str, Any],
) -> tuple[bool, bool, bool]:
    """Return file/live/browser bits for the internal agent attach payload."""

    vm_is_active = bool(
        isinstance(vm_context, dict)
        and vm_context.get("status") == "ready"
        and vm_context.get("ssh_host")
    )
    canvas_presentation_available = bool(
        not vm_is_active
        and remote_canvas_presentation_available(metadata, workspace_context)
    )
    # Port presentation is deliberately narrower than file presentation. The
    # positive bit is computed by the orchestrator from its default-off
    # deployment gate and the same attested workspace binding; the agent never
    # infers it from a backend label or endpoint reachability.
    from orchestrator.services.canvas_apps import canvas_live_preview_enabled

    canvas_live_apps_available = bool(
        canvas_presentation_available and canvas_live_preview_enabled()
    )
    from orchestrator.services.ssh_helpers import orchestrator_can_reach

    selected_context = vm_context if vm_is_active else workspace_context
    selected_host = (
        selected_context.get("ssh_host")
        or selected_context.get("host")
        or selected_context.get("pod_ip")
    )
    shared_browser_enabled = os.getenv(
        "CANVAS_SHARED_BROWSER_ENABLED", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
    try:
        selected_target_reachable = bool(
            selected_host and orchestrator_can_reach(str(selected_host))
        )
    except Exception:
        selected_target_reachable = False
    canvas_shared_browser_available = bool(
        canvas_presentation_available
        and shared_browser_enabled
        and selected_target_reachable
    )
    return (
        canvas_presentation_available,
        canvas_live_apps_available,
        canvas_shared_browser_available,
    )


async def attest_pinned_thread_k8s_workspace(
    thread_id: str,
    thread: Mapping[str, Any],
    metadata: Mapping[str, Any],
    workspace: Mapping[str, Any],
    binding: Mapping[str, Any],
    workspace_backend: str | None,
    *,
    dependencies: ThreadWorkspaceDeliveryDependencies,
) -> WorkspaceRuntimeAttestation | None:
    """Return exact Kubernetes SSH authority for a pinned session attach."""

    postgres_db = dependencies.store
    container_provisioner = dependencies.container_provisioner
    _thread_workspace_backend = dependencies.thread_workspace_backend

    if (
        thread.get("execution_lane") == "stateless"
        or workspace_backend != "sandbox"
        or workspace.get("status") != "ready"
    ):
        return None
    provisioner = str(workspace.get("provisioner") or "").strip().lower()
    if provisioner == "docker":
        return None
    if provisioner != "k8s":
        raise HTTPException(
            status_code=503,
            detail="Workspace provisioner authority is unavailable",
        )
    try:
        expected_runtime = str(
            UUID(str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY)))
        )
        binding_generation = str(UUID(str(binding.get("generation"))))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Workspace runtime authority is malformed",
        ) from exc
    if (
        binding.get("kind") != "remote"
        or workspace.get("_canvas_workspace_generation") != binding_generation
    ):
        raise HTTPException(
            status_code=503,
            detail="Workspace backing authority is unavailable",
        )
    try:
        attestation = await container_provisioner.attest_workspace_runtime(
            WorkspaceOwner.session(thread_id)
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Workspace runtime attestation is unavailable",
        ) from exc
    try:
        stored_port = int(workspace.get("port") or workspace.get("pod_port") or 30022)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Workspace endpoint authority is malformed",
        ) from exc
    if (
        attestation.runtime_incarnation != expected_runtime
        or binding.get("backing_id") != attestation.backing_id
        or binding.get("ssh_host_key_fingerprint")
        != attestation.ssh_host_key_fingerprint
        or str(workspace.get("pod_ip") or "") != attestation.pod_ip
        or stored_port != attestation.port
    ):
        raise HTTPException(
            status_code=409,
            detail="Workspace authority changed during attach",
        )

    refreshed = await postgres_db.get_thread(thread_id)
    if refreshed is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    refreshed_metadata = thread_metadata_object(refreshed)
    if (
        refreshed.get("execution_lane") == "stateless"
        or _thread_workspace_backend(refreshed) != "sandbox"
        or refreshed_metadata.get("workspace_container") != workspace
        or refreshed_metadata.get("_workspace_binding") != binding
    ):
        raise HTTPException(
            status_code=409,
            detail="Workspace authority changed during attach",
        )
    return attestation


async def attest_pinned_thread_vm_workspace(
    thread_id: str,
    thread: Mapping[str, Any],
    vm: Mapping[str, Any],
    workspace_backend: str | None,
    *,
    dependencies: ThreadWorkspaceDeliveryDependencies,
) -> WorkspaceRuntimeAttestation | None:
    """Pair the delivered VM snapshot with fresh controller/owner authority."""

    if thread.get("execution_lane") == "stateless" or workspace_backend != "vm":
        return None
    if not isinstance(vm, Mapping):
        raise HTTPException(409, "VM workspace authority is malformed")
    if vm.get("status") != "ready":
        return None
    try:
        attestation = await dependencies.vm_provisioner.attest_workspace_runtime(
            thread_id, entity_type="thread"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="VM workspace runtime attestation is unavailable",
        ) from exc
    # The adapter attests its own fresh owner read. It must also match the
    # snapshot used by this delivery, including the admitted host-key pin.
    if (
        vm.get("provision_generation") != attestation.workspace_generation
        or vm.get("active_pod_uid") != attestation.runtime_incarnation
        or vm.get("ssh_host_key_fingerprint") != attestation.ssh_host_key_fingerprint
        or vm.get("ssh_host") != attestation.host
        or str(vm.get("ssh_port")) != str(attestation.port)
        or vm.get("vm_uid") != attestation.vm_uid
    ):
        raise HTTPException(
            status_code=409,
            detail="VM workspace authority changed during attach",
        )
    refreshed = await dependencies.store.get_thread(thread_id)
    if refreshed is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    if (
        refreshed.get("execution_lane") != thread.get("execution_lane")
        or dependencies.thread_workspace_backend(refreshed) != workspace_backend
        or thread_metadata_object(refreshed).get("vm") != vm
    ):
        raise HTTPException(
            status_code=409,
            detail="VM workspace authority changed during attach",
        )
    return attestation


async def require_pinned_workspace_credential_owner(
    thread: dict[str, Any],
    presented_agent_id: str | None,
    presented_runtime_generation: str | None,
    presented_attach_token: str | None,
    *,
    expected_protected_ro_row: Mapping[str, Any] | None = None,
    dependencies: ThreadWorkspaceDeliveryDependencies,
) -> str | None:
    """Fence pinned workspace credentials to the reciprocal runtime owner."""

    postgres_db = dependencies.store
    _require_pinned_status_identity = dependencies.require_pinned_status_identity

    if str(thread.get("execution_lane") or "") != "pinned":
        return None
    raw_metadata = thread.get("metadata")
    if isinstance(raw_metadata, str):
        try:
            identity_metadata = json.loads(raw_metadata)
        except (json.JSONDecodeError, TypeError):
            identity_metadata = None
    elif raw_metadata is None:
        identity_metadata = {}
    else:
        identity_metadata = raw_metadata
    # Protected rows do not participate in the mixed-version grace period:
    # their response carries the live lower-mount credential. Any malformed
    # authority is protected-by-default and likewise requires an exact owner.
    protected_identity_required = (
        not isinstance(identity_metadata, dict)
        or protected_cloud_marker_state(identity_metadata) != "off"
    )
    exact_identity_required = bool(
        protected_identity_required or _require_pinned_status_identity()
    )
    if (
        not presented_agent_id
        or not presented_runtime_generation
        or (exact_identity_required and not presented_attach_token)
    ):
        if exact_identity_required:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "pinned_status_identity_required",
                    "message": (
                        "Pinned workspace credentials require agent, runtime "
                        "generation, and process attach identity."
                    ),
                },
            )
        return None
    try:
        parsed_agent_id = str(UUID(str(presented_agent_id)))
        parsed_runtime_generation = str(UUID(str(presented_runtime_generation)))
        parsed_attach_token = (
            str(UUID(str(presented_attach_token))) if presented_attach_token else None
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_runtime_identity_mismatch",
                "message": "Pinned workspace runtime ownership changed.",
            },
        ) from exc
    expected_authority = thread_runtime_authority(thread)
    expected_ro: dict[str, str | None] = {
        "mount_id": None,
        "engage_attempt": None,
        "grant_handle": None,
        "reader_id": None,
        "webdav_url": None,
    }
    if expected_protected_ro_row is not None:
        try:
            expected_ro = {
                "mount_id": str(UUID(str(expected_protected_ro_row["id"]))),
                "engage_attempt": str(
                    UUID(str(expected_protected_ro_row["engage_attempt"]))
                ),
                "grant_handle": str(expected_protected_ro_row["grant_handle"]),
                "reader_id": str(expected_protected_ro_row["reader_id"]),
                "webdav_url": str(expected_protected_ro_row["webdav_url"]),
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "pinned_runtime_identity_mismatch",
                    "message": "Pinned workspace runtime ownership changed.",
                },
            ) from exc
    if (
        expected_authority is None
        or expected_authority.generation != parsed_runtime_generation
        or str(thread.get("agent_id") or "") != parsed_agent_id
        or not await postgres_db.pinned_thread_agent_is_reciprocal(
            str(thread.get("id") or ""),
            parsed_agent_id,
            expected_runtime_generation=parsed_runtime_generation,
            expected_attach_token=parsed_attach_token,
            expected_ro_mount_id=expected_ro["mount_id"],
            expected_ro_engage_attempt=expected_ro["engage_attempt"],
            expected_ro_grant_handle=expected_ro["grant_handle"],
            expected_ro_reader_id=expected_ro["reader_id"],
            expected_ro_webdav_url=expected_ro["webdav_url"],
        )
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "pinned_runtime_identity_mismatch",
                "message": "Pinned workspace runtime ownership changed.",
            },
        )
    return parsed_agent_id


async def agent_get_thread_workspace_locked(
    thread_id: str,
    *,
    presented_agent_id: str | None = None,
    presented_runtime_generation: str | None = None,
    presented_attach_token: str | None = None,
    dependencies: ThreadWorkspaceDeliveryDependencies,
) -> dict[str, Any]:
    """Build a cold-session payload from state fetched under the DS lock."""

    # Bind every collaborator to the name the moved body already uses, so
    # the body below is byte-for-byte what `main` ran.  Two of them are
    # sibling operations in this module and take the same dependencies.
    GrantDenied = dependencies.GrantDenied
    LiteWorkspaceConfigError = dependencies.LiteWorkspaceConfigError
    _backend_from_override = dependencies.backend_from_override
    _build_agent_cloud_mount = dependencies.build_agent_cloud_mount
    _build_agent_cloud_sync = dependencies.build_agent_cloud_sync
    _build_protected_cloud_mount = dependencies.build_protected_cloud_mount
    _cloud_workspace_driver = dependencies.cloud_workspace_driver
    _grant_violations_detail = dependencies.grant_violations_detail
    _inject_lite_workspace_config = dependencies.inject_lite_workspace_config
    _inject_thread_dispatch_credentials = (
        dependencies.inject_thread_dispatch_credentials
    )
    _protected_cloud_delivery_state = dependencies.protected_cloud_delivery_state
    _protected_workspace_wait_payload = dependencies.protected_workspace_wait_payload
    _require_pinned_status_identity = dependencies.require_pinned_status_identity
    _resolve_session_config = dependencies.resolve_session_config
    _resolve_thread_datasources = dependencies.resolve_thread_datasources
    _resolve_thread_repositories = dependencies.resolve_thread_repositories
    _revalidate_thread_project_ids = dependencies.revalidate_thread_project_ids
    _ro_mount_matches_protected_selection = (
        dependencies.ro_mount_matches_protected_selection
    )
    _schedule_stateless_workspace_ensure = (
        dependencies.schedule_stateless_workspace_ensure
    )
    _thread_accepts_runtime = dependencies.thread_accepts_runtime
    _thread_project_ids = dependencies.thread_project_ids
    _thread_workspace_backend = dependencies.thread_workspace_backend
    _virtual_workspace_rclone_spec = dependencies.virtual_workspace_rclone_spec
    gitea_client = dependencies.gitea_client
    main_cloud_router = dependencies.cloud_router
    postgres_db = dependencies.store
    container_provisioner = dependencies.container_provisioner
    vm_workspaces_on_pod_network = dependencies.vm_workspaces_on_pod_network
    _agent_canvas_workspace_capabilities = agent_canvas_workspace_capabilities
    _attest_pinned_thread_k8s_workspace = functools.partial(
        attest_pinned_thread_k8s_workspace, dependencies=dependencies
    )
    _attest_pinned_thread_vm_workspace = functools.partial(
        attest_pinned_thread_vm_workspace, dependencies=dependencies
    )
    _require_pinned_workspace_credential_owner = functools.partial(
        require_pinned_workspace_credential_owner, dependencies=dependencies
    )
    thread = await postgres_db.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if not _thread_accepts_runtime(thread):
        raise HTTPException(
            status_code=409,
            detail=thread_runtime_refusal_detail(thread),
        )
    await _require_pinned_workspace_credential_owner(
        thread,
        presented_agent_id,
        presented_runtime_generation,
        presented_attach_token,
    )
    raw_metadata = thread.get("metadata")
    metadata = raw_metadata if raw_metadata is not None else {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            return _protected_workspace_wait_payload(
                state="failed", error_code="malformed_protected_marker"
            )
    if not isinstance(metadata, dict):
        return _protected_workspace_wait_payload(
            state="failed", error_code="malformed_protected_marker"
        )
    entry_protected_marker = protected_cloud_marker_state(metadata)
    if entry_protected_marker != "off":
        protected_state = await _protected_cloud_delivery_state(thread, metadata)
        if protected_state[0] != "ready":
            return _protected_workspace_wait_payload(
                state=protected_state[0], error_code=protected_state[1]
            )
    ws = metadata.get("workspace_container") or {}
    vm = metadata.get("vm") or {}
    binding = metadata.get("_workspace_binding") or {}
    # Keep the durable workspace snapshot separate from response-local
    # readiness redaction below.  The final credential-boundary read must
    # detect a real U1 -> U2 change, not mistake locally cleared coordinates
    # (restoring/non-ready/stale probe) for a concurrent database write.
    authority_ws = ws
    authority_vm = vm
    authority_binding = binding
    workspace_backend = _thread_workspace_backend(thread)
    if workspace_backend == "vm" and not isinstance(vm, dict):
        raise HTTPException(409, "VM workspace authority is malformed")
    if thread.get("execution_lane") == "stateless" and workspace_backend == "sandbox":
        workspace_status = str(ws.get("status") or "")
        restore_marker_present = WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY in ws
        raw_restore_required = ws.get(WORKSPACE_SNAPSHOT_RESTORE_REQUIRED_KEY)
        if restore_marker_present and type(raw_restore_required) is not bool:
            # This response carries SSH/cloud credentials. A malformed
            # present-only lifecycle sentinel is neither "no debt" nor safe
            # restore intent; refuse it before probing or scheduling effects.
            raise HTTPException(
                status_code=503,
                detail="Stateless snapshot restore authority is malformed",
            )
        snapshot_restore_required = raw_restore_required is True
        if snapshot_restore_required:
            # Container creation publishes its attested endpoint before a
            # snapshot restore has finished extracting into that endpoint.
            # The durable restore-intent bit is therefore part of readiness:
            # never hand a claimant an empty/partial tree while extraction is
            # still running (or after it failed).  The serialized lifecycle
            # owner clears this bit only after a successful reattach/extract.
            _schedule_stateless_workspace_ensure(thread_id)
            ws = {
                **ws,
                "status": "restoring",
                "pod_ip": None,
                "host": None,
                "port": None,
                "pod_port": None,
                "_canvas_workspace_generation": None,
                WORKSPACE_RUNTIME_INCARNATION_KEY: None,
            }
        elif workspace_status == "ready":
            # This internal poll is the physical attach credential boundary. A
            # cached Ready row is only evidence about the Pod UID that authored
            # it; a 404 or same-name replacement must not hand its stale SSH
            # endpoint to the claimant. Unknown control-plane health also fails
            # closed locally, but does not recreate anything until
            # absence/drift is confirmed.
            expected_runtime: str | None
            try:
                expected_runtime = str(
                    UUID(str(ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY)))
                )
            except (TypeError, ValueError):
                expected_runtime = None
            if expected_runtime is None:
                exact_pod_live: bool | None = False
            else:
                exact_pod_live = await container_provisioner.workspace_pod_live(
                    WorkspaceOwner.session(thread_id),
                    expected_runtime_incarnation=expected_runtime,
                )
            if exact_pod_live is True:
                # The Kubernetes read crossed an await point while workspace
                # lifecycle writers remained free to replace/invalidate the
                # row. Re-read and compare the complete endpoint attestation;
                # a delayed True for U1 must never authorize credentials after
                # durable state has moved to U2/non-ready.
                refreshed_thread = await postgres_db.get_thread(thread_id)
                if refreshed_thread is None:
                    raise HTTPException(status_code=404, detail="Thread not found")
                refreshed_metadata = thread_metadata_object(refreshed_thread)
                refreshed_ws = refreshed_metadata.get("workspace_container") or {}
                refreshed_binding = refreshed_metadata.get("_workspace_binding") or {}
                refreshed_backend = _thread_workspace_backend(refreshed_thread)
                if (
                    refreshed_thread.get("execution_lane") != "stateless"
                    or refreshed_backend != "sandbox"
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Workspace authority changed during attach",
                    )
                if refreshed_ws != ws or refreshed_binding != binding:
                    logger.info(
                        "Stateless workspace attestation changed during probe: "
                        "thread=%s old_runtime=%s new_runtime=%s",
                        thread_id,
                        expected_runtime,
                        refreshed_ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY),
                    )
                    thread = refreshed_thread
                    metadata = refreshed_metadata
                    ws = refreshed_ws
                    vm = metadata.get("vm") or {}
                    binding = refreshed_binding
                    authority_ws = refreshed_ws
                    authority_vm = vm
                    authority_binding = refreshed_binding
                    workspace_backend = refreshed_backend
                    refreshed_status = str(ws.get("status") or "")
                    if refreshed_status in {
                        "",
                        "none",
                        "deleted",
                        "failed",
                        "suspended",
                        "restoring",
                        "created",
                        "creating",
                        "pending",
                    }:
                        _schedule_stateless_workspace_ensure(thread_id)
                    ws = {
                        **ws,
                        "status": (
                            "creating"
                            if refreshed_status == "ready"
                            else refreshed_status
                        ),
                        "pod_ip": None,
                        "host": None,
                        "port": None,
                        "pod_port": None,
                        "_canvas_workspace_generation": None,
                        WORKSPACE_RUNTIME_INCARNATION_KEY: None,
                    }
                    # The refreshed U2/non-ready row has not been probed by
                    # this request. Do not reuse U1's result or start a Ready-U2
                    # ensure; the next poll will attest U2 directly.
                    exact_pod_live = None
            if exact_pod_live is not True:
                if exact_pod_live is False:
                    _schedule_stateless_workspace_ensure(thread_id)
                logger.info(
                    "Stateless workspace attestation pending: thread=%s probe=%r",
                    thread_id,
                    exact_pod_live,
                )
                # Response-local invalidation only. The single-flight ensure
                # owns durable lifecycle writes; meanwhile the agent keeps this
                # leased claim alive and polls again instead of accepting stale
                # SSH bytes.
                ws = {
                    **ws,
                    "status": "creating",
                    "pod_ip": None,
                    "host": None,
                    "port": None,
                    "pod_port": None,
                    "_canvas_workspace_generation": None,
                    WORKSPACE_RUNTIME_INCARNATION_KEY: None,
                }
        elif workspace_status in {
            "",
            "none",
            "deleted",
            "failed",
            "suspended",
            "restoring",
            "created",
            "creating",
            "pending",
        }:
            # A claimant can arrive after the original input-side ensure has
            # failed, or after a lifecycle sweep changed stale Ready evidence
            # to deleted. The durable input already exists, so this polling
            # boundary is the retry trigger; no second user action is required.
            # The required-runtime lifecycle arm also re-adopts a live
            # in-progress pod whose original readiness waiter was interrupted.
            _schedule_stateless_workspace_ensure(thread_id)
            ws = {
                **ws,
                "pod_ip": None,
                "host": None,
                "port": None,
                "pod_port": None,
                "_canvas_workspace_generation": None,
                WORKSPACE_RUNTIME_INCARNATION_KEY: None,
            }
    pinned_k8s_attestation = await _attest_pinned_thread_k8s_workspace(
        thread_id,
        thread,
        metadata,
        ws,
        binding,
        workspace_backend,
    )
    if pinned_k8s_attestation is not None:
        ws = {
            **ws,
            "host": pinned_k8s_attestation.host,
            "pod_ip": pinned_k8s_attestation.pod_ip,
            "port": pinned_k8s_attestation.port,
            WORKSPACE_RUNTIME_INCARNATION_KEY: (
                pinned_k8s_attestation.runtime_incarnation
            ),
        }
    pinned_vm_attestation = await _attest_pinned_thread_vm_workspace(
        thread_id, thread, vm, workspace_backend
    )
    workspace_generation: str | None = None
    workspace_runtime_incarnation: str | None = None
    workspace_ssh_host_key_fingerprint: str | None = None
    if isinstance(binding, dict):
        try:
            candidate_generation = str(UUID(str(binding.get("generation"))))
        except (TypeError, ValueError):
            candidate_generation = ""
        if (
            workspace_backend == "virtual"
            and binding.get("kind") == "virtual"
            and candidate_generation
        ):
            # A virtual generation is authoritative only for the deployment's
            # current durable object-store namespace.  Kind + UUID alone are
            # historical evidence: after an object-store rotation they could
            # attest bytes under the old ``threads/<id>/`` backing while this
            # claimant is attached to the new one.
            virtual_spec = _virtual_workspace_rclone_spec()
            if (
                isinstance(virtual_spec, dict)
                and virtual_spec.get("type") != "memory"
                and binding.get("backing_id")
                == virtual_thread_backing_id(thread_id, virtual_spec)
            ):
                workspace_generation = candidate_generation
        elif (
            workspace_backend != "virtual"
            and binding.get("kind") == "remote"
            and ws.get("status") == "ready"
            and str(ws.get("_canvas_workspace_generation") or "")
            == candidate_generation
        ):
            # A remote binding alone is historical evidence. Pair it with the
            # exact ready endpoint generation before cloud sync may call those
            # workspace bytes the source of an acknowledged generation.
            try:
                # Kubernetes publishes its Pod UID. Static Docker workspaces
                # have no Pod incarnation; their durable allocation lease UUID
                # is the exact runtime-incarnation authority. Both are paired
                # with the backing generation and host-key pin before any SSH
                # process-zero proof can be accepted.
                runtime_identity = (
                    ws.get("_docker_workspace_lease_id")
                    if ws.get("provisioner") == "docker"
                    else ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY)
                )
                candidate_runtime_incarnation = str(UUID(str(runtime_identity)))
            except (TypeError, ValueError):
                candidate_runtime_incarnation = None
            if thread.get("execution_lane") == "stateless":
                # For a movable claimant the complete ready authority is one
                # indivisible tuple. The fingerprint is the use-side fence:
                # stable Service DNS must not carry U1 authority onto a U2 pod
                # between this response and Paramiko's key exchange.
                if (
                    candidate_runtime_incarnation is not None
                    and remote_canvas_presentation_available(metadata, ws)
                ):
                    workspace_generation = candidate_generation
                    workspace_runtime_incarnation = candidate_runtime_incarnation
                    workspace_ssh_host_key_fingerprint = binding[
                        "ssh_host_key_fingerprint"
                    ]
            else:
                workspace_generation = candidate_generation
                workspace_runtime_incarnation = candidate_runtime_incarnation
                # Protected pinned sessions carry cloud credentials and staged
                # write authority. Bind their SSH use side to the same exact
                # host key as stateless claimants; stable Service/Docker
                # coordinates alone cannot distinguish a replacement runtime.
                if (
                    entry_protected_marker == "on"
                    and candidate_runtime_incarnation is not None
                    and remote_canvas_presentation_available(metadata, ws)
                ):
                    workspace_ssh_host_key_fingerprint = binding[
                        "ssh_host_key_fingerprint"
                    ]
    if pinned_k8s_attestation is not None:
        # The attestation identifies physical backing by PVC/Pod UID. Pinned
        # session consumers (shell ownership, cloud staging and retirement)
        # use the separately stored binding generation, already paired with
        # this exact attested backing and rechecked at the response boundary.
        workspace_generation = str(UUID(str(binding["generation"])))
        workspace_runtime_incarnation = pinned_k8s_attestation.runtime_incarnation
        workspace_ssh_host_key_fingerprint = (
            pinned_k8s_attestation.ssh_host_key_fingerprint
        )
    if pinned_vm_attestation is not None:
        # VM provision generation and launcher Pod UID are the admitted
        # physical pair; VM workspaces do not use the Canvas binding.
        workspace_generation = pinned_vm_attestation.workspace_generation
        workspace_runtime_incarnation = pinned_vm_attestation.runtime_incarnation
        workspace_ssh_host_key_fingerprint = (
            pinned_vm_attestation.ssh_host_key_fingerprint
        )
    if (
        entry_protected_marker == "on"
        and workspace_backend not in {"virtual", "none"}
        and ws.get("status") == "ready"
        and (
            workspace_generation is None
            or workspace_runtime_incarnation is None
            or workspace_ssh_host_key_fingerprint is None
        )
    ):
        return _protected_workspace_wait_payload(
            state="failed", error_code="workspace_runtime_identity_unavailable"
        )
    if (
        thread.get("execution_lane") == "stateless"
        and workspace_backend == "virtual"
        and workspace_generation is None
    ):
        # A virtual workspace is durable only through its orchestrator-owned
        # object-store binding.  Never hand a stateless claimant cloud
        # credentials with an unbound/ambiguous source namespace: the
        # generation fence could otherwise bless bytes from no authoritative
        # workspace incarnation.
        raise HTTPException(
            status_code=503,
            detail="Stateless virtual workspace binding is unavailable",
        )
    (
        canvas_presentation_available,
        canvas_live_apps_available,
        canvas_shared_browser_available,
    ) = _agent_canvas_workspace_capabilities(metadata, ws, vm)
    # Phase 1: project attachment + cloud mounts now live on thread_mounts.
    # Acknowledged-but-still-unavailable project drift is narrowed out
    # INSIDE _revalidate_thread_project_ids now, same as the warm-attach
    # call site — otherwise a cold-started session dies here on exactly the
    # drift the owner already acknowledged at resume — while a RECOVERED
    # acknowledged project returns automatically (spec §3.2).
    project_ids = await _revalidate_thread_project_ids(
        thread, await _thread_project_ids(thread_id)
    )
    datasources_payload = await _resolve_thread_datasources(
        thread, metadata, project_ids=project_ids
    )
    mount_rows = await postgres_db.list_thread_mounts(thread_id)
    suppress_disposable_cloud = bool(
        thread.get("execution_lane") == "stateless" and workspace_backend == "none"
    )
    if suppress_disposable_cloud:
        # ScratchBackend has no file tools and promises no durable workspace.
        # Do not advertise either the structured or legacy cloud surface: a
        # stateless claimant must not turn that explicitly disposable tier
        # into an accidental session-folder mirror. Pinned sessions retain
        # their historical cloud payload until that lane is retired/migrated.
        cloud_mount_cfg = None
        cloud_sync_cfg = None
    else:
        cloud_mount_cfg = await _build_agent_cloud_mount(
            thread,
            mount_rows=mount_rows,
            metadata=metadata,
        )
        if cloud_mount_cfg:
            cloud_sync_cfg = None
        elif metadata.get("protected_cloud"):
            # Protected thread with no engageable protected mount (flag off,
            # VM tier, or a refused/absent grant): NO live sync fallback of
            # any kind (fail-closed; agent sees degraded-cloud state, never a
            # live write path on a thread the user marked protected).
            cloud_sync_cfg = None
        elif _cloud_workspace_driver() == "rclone_mount":
            # rclone requested but unavailable/unsupported: fall back to the
            # regular session folder only. Do not eagerly clone thread_mounts
            # such as a default user home; that is the startup failure this
            # driver is meant to avoid.
            cloud_sync_cfg = _build_agent_cloud_sync(thread, mount_rows=[])
        else:
            cloud_sync_cfg = _build_agent_cloud_sync(thread, mount_rows=mount_rows)
    # Issue 13 follow-up: if the main cloud is up but this thread resolved NO
    # sync target (session-folder provisioning failed upstream, or user-home /
    # project-mount resolution produced nothing usable), the agent would
    # otherwise run unsynced with no signal. Flag it so the agent surfaces the
    # same degraded-sync state it shows for a failed initial pull, instead of
    # silently skipping cloud sync for the session's whole life.
    try:
        _cloud_up = main_cloud_router.active.is_initialized
    except Exception:
        _cloud_up = False
    cloud_sync_degraded = bool(
        not suppress_disposable_cloud
        and _cloud_up
        and not cloud_mount_cfg
        and not cloud_sync_cfg
        and (metadata.get("protected_cloud") or not thread.get("nc_session_folder"))
    )
    # Re-inject credentials in-flight: the persisted config_override is stripped
    # of secrets (redact_config_override at create/hot-swap). This endpoint is
    # the agent's key source on resume — its attach fallback in persistent_app.py
    # reads ``config_override`` from here. require_internal + ingress-stripped, so
    # plaintext stays on the agent trust boundary. Models/providers survive
    # stripping, so user_settings isn't needed to repopulate the keys.
    co = metadata.get("config_override") or {}
    include_kb_profile = bool(project_ids) or any(
        str(datasource.get("type") or "").lower() == "kb"
        for datasource in datasources_payload or []
    )
    if co or include_kb_profile:
        co = await _inject_thread_dispatch_credentials(
            co,
            user_id=str(thread["user_id"]) if thread.get("user_id") else None,
            project_id=str(thread["project_id"]) if thread.get("project_id") else None,
            include_kb_profile=include_kb_profile,
        )
    # Orchestrator-resolved config for cold/dedicated attach: the agent prefers
    # this fully-resolved, credential-injected blob over the config_override
    # merge above (which stays for the fallback). None when experts are off.
    # Session dispatch PEP (fail closed): a grant denial or resolve error must not
    # fall through to the unvetted config_override — refuse the attach (403).
    _sess_status: dict[str, Any] = {"_capture_manifest": True}
    try:
        session_resolved = await _resolve_session_config(
            thread, metadata, status=_sess_status
        )
    except GrantDenied as gd:
        raise HTTPException(
            status_code=403, detail=_grant_violations_detail(gd.violations)
        )
    if _sess_status.get("state") == "error":
        raise HTTPException(
            status_code=403,
            detail="capability grants could not be verified for this session config",
        )
    if (
        thread.get("execution_lane") == "pinned"
        and thread.get("status") == "created"
        and dependencies.vm_provisioner is not None
        and vm.get("status") != "ready"
    ):
        from orchestrator.services.vm_thread_retained_resume import (
            ensure_retained_thread_vm,
        )

        retained = await ensure_retained_thread_vm(
            thread,
            store=postgres_db,
            provisioner=dependencies.vm_provisioner,
        )
        if retained is not None:
            current = await postgres_db.get_thread(thread_id)
            if not _thread_accepts_runtime(current):
                raise HTTPException(409, "Retained VM runtime changed during admission")
            await _require_pinned_workspace_credential_owner(
                current,
                presented_agent_id,
                presented_runtime_generation,
                presented_attach_token,
            )
            return initial_vm_wait_payload(current)

    if (
        thread.get("execution_lane") == "pinned"
        and thread.get("status") == "created"
        and workspace_backend == "vm"
        and dependencies.vm_provisioner is not None
        and vm.get("status") != "ready"
        and is_initial_thread_vm_poll(thread, vm)
    ):
        # Admission follows the real binding and current authorization. A
        # restart between binding and this poll simply repeats this boundary.
        # No lifecycle lock may be nested under the caller's datasource lock.
        if not all(
            (presented_agent_id, presented_runtime_generation, presented_attach_token)
        ):
            raise HTTPException(
                409, "Initial VM creation requires exact runtime identity"
            )
        if metadata.get("vm") is None:
            current = await ensure_initial_thread_vm(
                thread,
                store=postgres_db,
                provisioner=dependencies.vm_provisioner,
            )
        else:
            if (
                vm.get("status")
                not in {
                    "provisioning",
                    "created",
                    "starting",
                    "ssh_pending",
                    "failed",
                    "waiting_capacity",
                    "waiting_golden",
                    "waiting_headscale",
                    "waiting_preparation",
                }
                or not vm.get("provision_generation")
                or vm.get("rootdisk") == "kept"
            ):
                raise HTTPException(409, "VM workspace is not an initial creation")
            current = await postgres_db.get_thread(thread_id)
        if not _thread_accepts_runtime(current):
            raise HTTPException(409, "Initial VM runtime changed during admission")
        await _require_pinned_workspace_credential_owner(
            current,
            presented_agent_id,
            presented_runtime_generation,
            presented_attach_token,
        )
        if thread_metadata_object(current).get("vm") is not None:
            await require_current_initial_vm_source(current, store=postgres_db)
        return initial_vm_wait_payload(current)
    # Lite (virtual/none) sessions run with no workspace pod. Attach the
    # object-store mounts in-flight here — the same enrichment
    # _send_session_attach does for the idle-pool path — so a DEDICATED session
    # agent (provisioned when no pool agent is free) can build its lite backend
    # from this response. Without it _attach_session would poll for a workspace
    # pod that never exists and the agent would exit cleanly (the lite session
    # boot gap, no_workspace_agent_mode). No-op for sandbox/vm.
    try:
        co = _inject_lite_workspace_config(co, prefix=f"threads/{thread_id}/") or co
        # The resolved blob is the agent's PREFERRED hydration source, loaded via
        # load_agent_config_from_dict(resolved["agent"]) WITHOUT a config_override
        # merge (persistent_app._attach_session). Its agent.workspace already
        # carries the lite backend but NOT the in-flight object-store mounts —
        # attach them there, else create_lite_backend raises "requires
        # workspace.mounts" and the lite session can't boot.
        if isinstance(session_resolved, dict) and _backend_from_override(co) in (
            LITE_BACKENDS
        ):
            agent_ws = session_resolved.setdefault("agent", {}).setdefault(
                "workspace", {}
            )
            agent_ws.update(co.get("workspace") or {})
    except LiteWorkspaceConfigError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    repositories_payload = await _resolve_thread_repositories(project_ids)
    git_remote_url = ws.get("git_remote_url")
    managed_repository_credentials: list[dict[str, Any]] | None = None
    runtime_repository_backend: str | None = None
    if vm.get("status") == "ready" and vm.get("ssh_host"):
        runtime_repository_backend = "vm"
    elif ws.get("status") == "ready" and (ws.get("pod_ip") or ws.get("host")):
        runtime_repository_backend = "sandbox"
    if runtime_repository_backend is not None:
        try:
            (
                git_remote_url,
                repositories_payload,
                managed_repository_credentials,
            ) = await authorize_thread_repository_transport(
                postgres_db,
                gitea_client,
                thread,
                repositories_payload,
                backend=runtime_repository_backend,
            )
        except ManagedRepositoryAuthorityError as exc:
            logger.warning(
                "Thread repository authority unavailable at attach: thread=%s code=%s",
                thread_id,
                exc.code,
            )
            raise HTTPException(
                status_code=503,
                detail="Workspace repository authority is unavailable",
            ) from exc
    if runtime_repository_backend == "vm" and not vm_workspaces_on_pod_network():
        for repository in repositories_payload or []:
            if not repository.get("is_managed") and repository.get("repo_url"):
                repository["repo_url"] = externalize_gitea_url(repository["repo_url"])
    if not await postgres_db.managed_repository_authorities_are_current(
        managed_repository_credentials
    ):
        raise HTTPException(
            status_code=503,
            detail="Workspace repository authority changed during attach",
        )
    if pinned_k8s_attestation is not None:
        latest = await postgres_db.get_thread(thread_id)
        if latest is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        latest_metadata = thread_metadata_object(latest)
        confirmed = await _attest_pinned_thread_k8s_workspace(
            thread_id,
            latest,
            latest_metadata,
            latest_metadata.get("workspace_container") or {},
            latest_metadata.get("_workspace_binding") or {},
            _thread_workspace_backend(latest),
        )
        if confirmed != pinned_k8s_attestation:
            raise HTTPException(
                status_code=409,
                detail="Workspace authority changed during attach",
            )
    if pinned_vm_attestation is not None:
        latest = await postgres_db.get_thread(thread_id)
        if latest is None:
            raise HTTPException(status_code=404, detail="Thread not found")
        confirmed = await _attest_pinned_thread_vm_workspace(
            thread_id,
            latest,
            thread_metadata_object(latest).get("vm") or {},
            _thread_workspace_backend(latest),
        )
        if confirmed != pinned_vm_attestation:
            raise HTTPException(
                status_code=409,
                detail="VM workspace authority changed during attach",
            )
    # Complete protected-reader and exact selected-mount validation before the
    # final lifecycle read. The endpoint holds ``thread_datasource_lock`` for
    # this whole function, so selection cannot change after this snapshot;
    # marker/lifecycle can still change through independent terminal writers
    # and are checked synchronously after the final read below.
    prepared_protected_mount: dict[str, Any] | None = None
    if entry_protected_marker == "on":
        prepared_runtime_authority = thread_runtime_authority(thread)
        if prepared_runtime_authority is None:
            return _protected_workspace_wait_payload(state="engaging")
        prepared_ro_row, prepared_mount_rows = await asyncio.gather(
            postgres_db.get_ro_mount_by_thread(thread_id),
            postgres_db.list_thread_mounts(thread_id),
        )
        if not _ro_mount_matches_protected_selection(
            prepared_ro_row,
            prepared_mount_rows,
            thread_id=thread_id,
            user_id=str(thread.get("user_id") or ""),
            runtime_generation=prepared_runtime_authority.generation,
        ):
            return _protected_workspace_wait_payload(state="engaging")
        prepared_protected_mount = (
            _build_protected_cloud_mount(prepared_ro_row, thread_id=thread_id)
            if prepared_ro_row
            else None
        )
        if prepared_protected_mount is None:
            return _protected_workspace_wait_payload(state="engaging")

    # Everything above this point crosses policy, workspace, repository and
    # cloud awaits. This is the actual credential-delivery boundary and the
    # final await before returning coordinates.
    if dependencies.capture_session_config is not None:
        session_resolved = await dependencies.capture_session_config(
            thread, session_resolved, _sess_status, project_ids=project_ids
        )
    final_thread = await postgres_db.get_thread(thread_id)
    if not _thread_accepts_runtime(final_thread):
        raise HTTPException(
            status_code=409,
            detail=thread_runtime_refusal_detail(final_thread),
        )
    # This is deliberately the last await before any credential-bearing
    # response. A delayed old runtime A that crosses End -> Resume -> B sees
    # the successor binding here and is refused without learning B's bytes.
    await _require_pinned_workspace_credential_owner(
        final_thread,
        presented_agent_id,
        presented_runtime_generation,
        presented_attach_token,
        expected_protected_ro_row=(
            prepared_ro_row if entry_protected_marker == "on" else None
        ),
    )
    final_raw_metadata = final_thread.get("metadata")
    final_metadata = final_raw_metadata if final_raw_metadata is not None else {}
    if isinstance(final_metadata, str):
        try:
            final_metadata = json.loads(final_metadata)
        except (json.JSONDecodeError, TypeError):
            return _protected_workspace_wait_payload(
                state="failed", error_code="malformed_protected_marker"
            )
    if not isinstance(final_metadata, dict):
        return _protected_workspace_wait_payload(
            state="failed", error_code="malformed_protected_marker"
        )
    final_ws = final_metadata.get("workspace_container") or {}
    final_vm = final_metadata.get("vm") or {}
    final_binding = final_metadata.get("_workspace_binding") or {}
    if (
        final_ws != authority_ws
        or final_vm != authority_vm
        or final_binding != authority_binding
        or (
            pinned_vm_attestation is not None
            and (
                final_thread.get("execution_lane") != thread.get("execution_lane")
                or _thread_workspace_backend(final_thread) != workspace_backend
            )
        )
    ):
        # Workspace U1 -> U2 is independent of the thread generation. Never
        # splice U1 coordinates/credentials into a response authorized by a
        # final read of U2; the agent retries from one coherent snapshot.
        raise HTTPException(
            status_code=409,
            detail={"code": "workspace_runtime_identity_changed"},
        )
    final_protected_marker = protected_cloud_marker_state(final_metadata)
    if final_protected_marker == "malformed":
        return _protected_workspace_wait_payload(
            state="failed", error_code="malformed_protected_marker"
        )
    if final_protected_marker != entry_protected_marker:
        # A settings/lifecycle writer crossed this expensive credential build.
        # Return no coordinates from either snapshot; the next poll starts
        # from one authoritative marker instead of splicing the two states.
        return _protected_workspace_wait_payload(state="engaging")
    if final_protected_marker == "on":
        # All DB/cloud validation already completed under the selection lock;
        # only the exact prepared mount may cross the response boundary.
        if prepared_protected_mount is None:
            return _protected_workspace_wait_payload(state="engaging")
        cloud_mount_cfg = prepared_protected_mount
        cloud_sync_cfg = None
        cloud_sync_degraded = False
    final_runtime_authority = thread_runtime_authority(final_thread)
    if final_runtime_authority is None and (
        final_protected_marker != "off" or _require_pinned_status_identity()
    ):
        raise HTTPException(
            status_code=409,
            detail={"code": "pinned_runtime_generation_unavailable"},
        )

    return {
        "status": ws.get("status", "none"),
        "pinned_status_identity_contract": 1,
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": (
            final_runtime_authority.generation
            if final_runtime_authority is not None
            else None
        ),
        # Internal claim/attach identity, never exposed by owner-facing thread
        # routes. Durable cloud generations bind their source bytes to this
        # orchestrator-owned workspace incarnation and recheck it before each
        # external PUT.
        "workspace_generation": workspace_generation,
        # Separate from the backing generation: a PVC-backed workspace keeps
        # its generation across pod replacement, while this Kubernetes Pod UID
        # changes and fences the prior runtime's shell ownership record.
        "workspace_runtime_incarnation": workspace_runtime_incarnation,
        # Internal stateless-attach authority only. It is emitted iff the exact
        # ready generation/runtime tuple above is accepted, and is consumed as
        # an exact Paramiko host-key pin before any SFTP, exec, or tmux claim.
        "workspace_ssh_host_key_fingerprint": (workspace_ssh_host_key_fingerprint),
        "workspace_provisioner": (
            str(vm.get("provisioner") or "vm")
            if vm.get("status") == "ready" and vm.get("ssh_host")
            else (str(ws.get("provisioner") or "") or None)
        ),
        # K8s provisioner uses pod_ip; Docker provisioner uses host — normalize
        "pod_ip": ws.get("pod_ip") or ws.get("host"),
        "pod_name": ws.get("pod_name"),
        "pod_port": ws.get("pod_port") or ws.get("port"),
        "namespace": ws.get("namespace"),
        "git_remote_url": git_remote_url,
        "managed_repository_credentials": managed_repository_credentials,
        # Public capability only. A ready endpoint without a paired trusted
        # binding must not cause the agent to advertise Canvas tools which can
        # never work.
        "canvas_presentation_available": canvas_presentation_available,
        "canvas_live_apps_available": canvas_live_apps_available,
        "canvas_shared_browser_available": canvas_shared_browser_available,
        # SSH key path (set by Docker provisioner in dev mode)
        "ssh_key_path": os.environ.get("SSH_KEY_PATH"),
        # VM fields (take precedence when present)
        "vm_status": (final_metadata.get("workspace_preparation") or vm).get("status"),
        "vm_preparation": (final_metadata.get("workspace_preparation") or vm).get(
            "preparation"
        ),
        "vm_preparation_timeout_s": preparation_wait_budget(co, vm),
        "vm_ssh_host": vm.get("ssh_host"),
        "vm_ssh_port": vm.get("ssh_port"),
        "vm_name": vm.get("vm_name"),
        # Config overrides (model, temperature, etc.) — secrets re-injected above
        "config_override": co,
        # Orchestrator-resolved config blob (preferred over config_override when present)
        "resolved_config": session_resolved,
        # Project scoping
        "project_ids": project_ids,
        # Resolved datasources for the thread
        "datasources": datasources_payload,
        # Raw project repository payload for internal session tools. Public
        # repository routes intentionally redact clone credentials.
        "repositories": repositories_payload,
        # Nextcloud session folder (legacy; preserved one release for back-compat)
        "nc_session_folder": (
            None
            if suppress_disposable_cloud or final_protected_marker != "off"
            else final_thread.get("nc_session_folder")
        ),
        # Structured cloud-sync config (backend + webdav URL + auth).
        # Agent consumes this via ``src.services.cloud_sync.build_workspace_sync``.
        "cloud_sync": cloud_sync_cfg,
        # Structured lazy cloud mount config. Mutually exclusive with
        # cloud_sync for the same thread response.
        "cloud_mount": cloud_mount_cfg,
        # True when cloud is up but no sync target resolved (Issue 13 follow-up).
        "cloud_sync_degraded": cloud_sync_degraded,
        # Protected Cloud Mode marker (F-C1): tells the agent to fail-close
        # the legacy nc_session_folder sync shim and any cloud_sync
        # consumption rather than falling back to a live WebDAV mount.
        "protected_cloud": final_protected_marker != "off",
        "protected_cloud_state": ("ready" if final_protected_marker == "on" else None),
        "protected_cloud_error_code": None,
    }


__all__ = [
    "ThreadWorkspaceDeliveryDependencies",
    "agent_canvas_workspace_capabilities",
    "agent_get_thread_workspace_locked",
    "attest_pinned_thread_k8s_workspace",
    "attest_pinned_thread_vm_workspace",
    "require_pinned_workspace_credential_owner",
]
