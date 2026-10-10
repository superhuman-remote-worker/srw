"""Cloud payloads handed to an agent runtime: ``cloud_sync`` and ``cloud_mount``.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane M). Three
properties are load-bearing and moved unchanged:

* **Auth payloads never reach a log.** ``_backend_cloud_cfg`` and the rclone
  builders assemble WebDAV credentials, reader passwords and Keycloak client
  secrets into the returned dict and nothing else — every log line here names
  a mount, a thread or an error kind, never the payload.
* **The protected branch is entered by the marker alone.** Whether protected
  cloud mode is *enabled* only decides between "no mount" and "a protected
  mount"; it can never route a protected-marked thread into the live builders,
  which would hand it agent-service credentials.
* **All-or-fallback.** ``_build_agent_cloud_mount`` mounts every requested
  ``thread_mounts`` row or none of them; a partial set falls back to the
  legacy session folder so a default user-home row cannot silently degrade
  into the eager clone path.

Collaborators arrive through :class:`AgentCloudMountDependencies`, rebuilt per
invocation by the application rather than captured at import: the main-cloud
router and the postgres pool are both rebound during ``lifespan``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional

from orchestrator.services.cloud import (
    PROTECTED_PROJECT_FOLDER,
    CloudBackendError,
    CloudMountSubject,
    ProjectFolderHandle,
    SessionFolderHandle,
    SupportsProtectedLower,
    SupportsRcloneMount,
    provider_adapter,
    provider_offers,
)
from orchestrator.services.cloud_mount_sidecar import (
    agent_payload,
    recorded_sidecar_plan,
)
from orchestrator.services.sandbox_workspace_settings import container_denies_fuse
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentCloudMountDependencies:
    """Collaborators for one cloud-payload build, resolved per invocation.

    ``store`` is main's ``postgres_db`` and ``cloud_router`` its
    ``main_cloud_router``; both are rebound during ``lifespan``, so the
    application rebuilds this dataclass per call rather than capturing it at
    import. ``cloud_tasks`` is the application-owned ``CloudTaskRegistry``
    (its ``protected_engage_get`` half is the only part used here) — it
    replaces main's ``_protected_engage_tasks`` module dict, and holding the
    reference is what fixes the asyncio "fire-and-forget task can be GC'd
    mid-flight" hazard.
    """

    store: Any
    cloud_router: Any
    cloud_tasks: Any
    is_protected_cloud_mode_enabled: Callable[[], bool]
    cloud_workspace_driver: Callable[[], str]
    slugify_mount_name: Callable[[str], str]


def _resolve_cloud_session_url(
    thread: dict[str, Any],
    mount_rows: list[dict[str, Any]] | None = None,
    *,
    dependencies: AgentCloudMountDependencies,
) -> Optional[str]:
    """Compute a backend-agnostic browser URL for a thread's cloud folder.

    Looks at the legacy session-folder handle first (Phase 1 and earlier).
    When that's empty — as happens for Phase 2 default-project threads
    where the session folder is intentionally skipped in favor of the
    user-home mount — fall back to the first ``project_default`` mount
    row's cloud handle. The Cockpit's "Cloud" button (sessions-page line
    179: ``thread.cloud_session_url || thread.nc_session_folder``) depends
    on this; without the fallback, default-project threads show no
    button even though sync is working fine.
    """
    handle_str = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    if handle_str:
        backend = dependencies.cloud_router.for_thread_optional(thread)
        if backend is None or not backend.is_initialized:
            return None
        try:
            handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
            return backend.get_session_folder_browser_url(handle)
        except Exception:
            return None

    # No legacy folder — try the project_default mount.
    for m in mount_rows or []:
        if m.get("mount_kind") != "project_default":
            continue
        row_backend_id = m.get("backend_id")
        row_handle_str = m.get("cloud_handle")
        if not row_backend_id or not row_handle_str:
            continue
        try:
            backend = dependencies.cloud_router.for_backend_instance(
                str(m.get("backend_instance_id") or ""),
                expected_backend_id=str(row_backend_id),
            )
        except Exception:
            continue
        if not backend.is_initialized:
            continue
        try:
            handle = ProjectFolderHandle.from_db(
                row_handle_str, backend=backend.backend_id
            )
            return backend.get_project_folder_browser_url(handle)
        except Exception:
            continue
    return None


def _backend_cloud_cfg(
    backend,
    webdav_url: str,
    *,
    target_user_sub: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Build a single ``{backend, webdav_url, auth}`` cfg for the agent.

    Shape matches ``src/services/cloud_sync/__init__.py::build_workspace_sync``'s
    expected payload. Returns ``None`` when credentials aren't resolvable.
    Never logs the auth payload.

    The provider's adapter builds it (``cloud_sync_config``): which credential
    authenticates WebDAV is the adapter's business. ``target_user_sub`` names
    the owner of a user-home folder (Phase 2), for a provider that acts as
    that user.
    """
    return backend.cloud_sync_config(webdav_url, target_user_sub=target_user_sub)


def _build_agent_cloud_sync(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None = None,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    """Build the ``cloud_sync`` payload the agent uses to push/pull workspace files.

    Phase 1 of ``knowledge-base/knowledge/features/cloud_collaboration_model.md`` §9 introduces the
    multi-mount model. Payload shape is now ``version: 2``::

        {
          "version": 2,
          "session_folder": {backend, webdav_url, auth} | None,
          "mounts": [
              {mount_id, mount_kind, target_path, backend, webdav_url, auth},
              ...
          ],
        }

    ``session_folder`` is the legacy per-thread session folder cfg (kept in
    parallel for back-compat until Phase 4). ``mounts`` are the project /
    user-home / repo mounts derived from ``thread_mounts``. Returns ``None``
    when neither a session folder nor any mount could be resolved — in that
    case there's nothing for the agent to sync. Never logs auth payloads.
    """
    # ---- legacy session folder (still provisioned in v1)
    session_folder_cfg: Optional[dict[str, Any]] = None
    handle_str = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    if handle_str:
        backend = dependencies.cloud_router.for_thread_optional(thread)
        if backend is not None and backend.is_initialized:
            try:
                handle = SessionFolderHandle.from_db(
                    handle_str, backend=backend.backend_id
                )
                webdav_url = backend.get_session_folder_webdav_url(handle)
                if webdav_url:
                    session_folder_cfg = _backend_cloud_cfg(backend, webdav_url)
            except Exception:
                session_folder_cfg = None

    # ---- new-style mounts from thread_mounts
    mounts_out: list[dict[str, Any]] = []
    for row in mount_rows or []:
        row_backend_id = row.get("backend_id")
        webdav_url = row.get("webdav_url")
        if not row_backend_id or not webdav_url:
            # The orchestrator couldn't resolve this mount's transport
            # details — skip rather than ship a half-built entry. Agent
            # will surface "no mount available" via the raise-and-block
            # policy at the next turn boundary if anything depended on it.
            continue
        try:
            backend = dependencies.cloud_router.for_backend_instance(
                str(row.get("backend_instance_id") or ""),
                expected_backend_id=str(row_backend_id),
            )
        except Exception:
            continue
        if not backend.is_initialized:
            continue
        cfg = _backend_cloud_cfg(
            backend,
            webdav_url,
            target_user_sub=row.get("target_user_sub"),
        )
        if not cfg:
            continue
        mounts_out.append(
            {
                "mount_id": str(row.get("id", "")),
                "mount_kind": row.get("mount_kind"),
                "target_path": row.get("target_path", ""),
                **cfg,
            }
        )

    if not session_folder_cfg and not mounts_out:
        return None

    return {
        "version": 2,
        "session_folder": session_folder_cfg,
        "mounts": mounts_out,
    }


def _vm_runtime_ready(metadata: dict[str, Any]) -> bool:
    vm_ctx = metadata.get("vm") or {}
    return vm_ctx.get("status") == "ready" and bool(vm_ctx.get("ssh_host"))


def _runtime_supports_rclone_mount(
    metadata: dict[str, Any], *, dependencies: AgentCloudMountDependencies
) -> bool:
    """Whether this thread's current workspace runtime may receive cloud_mount."""
    if dependencies.cloud_workspace_driver() != "rclone_mount":
        return False
    if _vm_runtime_ready(metadata):
        return True
    allow_container = os.getenv("CLOUD_RCLONE_ALLOW_CONTAINER", "true").lower()
    if allow_container in {"0", "false", "no", "off"}:
        return False
    ws_ctx = metadata.get("workspace_container") or {}
    return ws_ctx.get("status") == "ready" and bool(
        ws_ctx.get("pod_ip") or ws_ctx.get("host")
    )


def _runtime_supports_terminal_rclone_retirement(
    thread: dict[str, Any],
    metadata: dict[str, Any],
    *,
    terminal_token: int,
    dependencies: AgentCloudMountDependencies,
) -> bool:
    """Whether End may reconstruct rclone identity for exact-runtime cleanup.

    ``retiring_process_zero`` must never enter the ordinary delivery gate: it
    is an absorbing no-new-work projection.  Terminal cleanup nevertheless
    needs the byte-identical non-secret mount specification so it can adopt
    and remove an already-resident rclone process.  Bind that exception to the
    same ended thread, pending token, workspace generation, Pod UID, and SSH
    fingerprint that the terminal shell protocol accepts.
    """

    if (
        dependencies.cloud_workspace_driver() != "rclone_mount"
        or thread.get("status") != "ended"
        or thread.get("execution_lane") != "stateless"
        or thread_metadata_object(thread) != metadata
    ):
        return False
    allow_container = os.getenv("CLOUD_RCLONE_ALLOW_CONTAINER", "true").lower()
    if allow_container in {"0", "false", "no", "off"}:
        return False
    workspace = metadata.get("workspace_container") or {}
    if not isinstance(workspace, dict) or workspace.get("status") not in {
        "ready",
        "retiring_process_zero",
    }:
        return False

    from orchestrator.services.stateless_session_retirement import (
        resolve_shell_retirement_authority,
    )
    from shared.session_retirement import stateless_retirement_authority

    try:
        retirement = stateless_retirement_authority(metadata)
        authority = resolve_shell_retirement_authority(
            thread, terminal_token=terminal_token
        )
    except (RuntimeError, TypeError, ValueError):
        return False
    return bool(
        retirement is not None
        and retirement.get("terminal_token") == terminal_token
        and retirement.get("claimant_quiesced") is True
        and retirement.get("resident_cleanup_required") is True
        and retirement.get("workspace_generation") == authority.workspace_generation
        and retirement.get("endpoint_generation") == authority.workspace_generation
        and retirement.get("runtime_incarnation") == authority.runtime_incarnation
        and retirement.get("host_key_fingerprint") == authority.host_key_fingerprint
    )


def _cloud_mount_name(
    row: dict[str, Any],
    used: set[str],
    *,
    dependencies: AgentCloudMountDependencies,
) -> str:
    if row.get("mount_kind") == "project_default":
        base = "home"
    else:
        target = str(row.get("target_path") or "").strip("/")
        base = target.rsplit("/", 1)[-1] if target else "cloud"
        base = dependencies.slugify_mount_name(base)
    name = base
    suffix = 2
    while name in used:
        name = f"{base}-{suffix}"
        suffix += 1
    used.add(name)
    return name


async def _build_rclone_mount_from_row(
    row: dict[str, Any],
    *,
    workspace_name: str,
    runtime_is_vm: bool = False,
    dependencies: AgentCloudMountDependencies,
    failures: list[str] | None = None,
) -> Optional[dict[str, Any]]:
    """One row's rclone mount, or ``None``; ``failures`` gets why, as a
    closed code (``no_transport``, ``backend_unavailable``, a
    ``CloudBackendErrorKind`` value, or ``error``), never the error text."""

    def refused(reason: str) -> None:
        if failures is not None:
            failures.append(reason)

    backend_id = row.get("backend_id")
    handle_str = row.get("cloud_handle")
    if not backend_id or not handle_str:
        refused("no_transport")
        return None
    try:
        backend = dependencies.cloud_router.for_backend_instance(
            str(row.get("backend_instance_id") or ""),
            expected_backend_id=str(backend_id),
        )
    except Exception:
        refused("backend_unavailable")
        return None
    if not backend.is_initialized or not isinstance(backend, SupportsRcloneMount):
        refused("backend_unavailable")
        return None
    try:
        handle = ProjectFolderHandle.from_db(handle_str, backend=backend.backend_id)
        subject = CloudMountSubject(
            user_sub=row.get("target_user_sub"),
            username=handle.vendor_meta.get("username"),
        )
        target_path = f"/cloud/{workspace_name}"
        # vm tier = root → read-only by default (root + FUSE over the whole
        # Space is a real blast radius); see
        # knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md § Security.
        access = "read_only" if runtime_is_vm else "read_write"
        spec = await backend.build_rclone_mount_spec(
            handle=handle,
            mount_kind=str(row.get("mount_kind") or "project"),
            target_path=target_path,
            access=access,
            subject=subject,
            prefer_public_url=runtime_is_vm,
        )
    except CloudBackendError as e:
        logger.info(
            "Thread mount %s cannot use rclone (%s); considering fallback.",
            row.get("id") or row.get("source_ref") or row.get("mount_kind"),
            e.kind.value,
        )
        refused(str(e.kind.value))
        return None
    except Exception as e:
        logger.warning(
            "Thread mount %s: failed to build rclone spec: %s",
            row.get("id") or row.get("source_ref") or row.get("mount_kind"),
            e,
        )
        refused("error")
        return None

    return {
        "mount_id": str(row.get("id") or row.get("source_ref") or workspace_name),
        "mount_kind": row.get("mount_kind"),
        "backend": backend.backend_id,
        "target_path": target_path,
        "workspace_name": workspace_name,
        "access": access,
        "source_ref": str(row.get("source_ref")) if row.get("source_ref") else None,
        **spec.to_payload(),
    }


async def _build_rclone_session_mount(
    thread: dict[str, Any],
    *,
    runtime_is_vm: bool = False,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    handle_str = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    if not handle_str:
        return None
    backend = dependencies.cloud_router.for_thread_optional(thread)
    if (
        backend is None
        or not backend.is_initialized
        or not isinstance(backend, SupportsRcloneMount)
    ):
        return None
    try:
        handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
        access = "read_only" if runtime_is_vm else "read_write"
        spec = await backend.build_rclone_mount_spec(
            handle=handle,
            mount_kind="session_folder",
            target_path="/cloud/home",
            access=access,
            subject=None,
            prefer_public_url=runtime_is_vm,
        )
    except Exception as e:
        logger.warning(
            "Thread %s: failed to build rclone session-folder fallback: %s",
            thread.get("id"),
            e,
        )
        return None
    return {
        "mount_id": "legacy-session",
        "mount_kind": "session_folder",
        "backend": backend.backend_id,
        "target_path": "/cloud/home",
        "workspace_name": "home",
        "access": access,
        **spec.to_payload(),
    }


# Protected-mode overlay layout (design §11.3): upper/work INSIDE the snapshot
# scope (/home/agent-host), merged mount + raw rclone lower OUTSIDE it.
_PROTECTED_OVERLAY_UPPER = "/home/agent-host/.overlay/upper"
_PROTECTED_OVERLAY_WORK = "/home/agent-host/.overlay/work"
_PROTECTED_OVERLAY_MERGED = "/cloud/merged"
_PROTECTED_LOWER_TARGET = "/cloud/lower"
_PROTECTED_UPPERDIR_QUOTA_BYTES = 8 * 1024 * 1024 * 1024  # < 10Gi emptyDir cliff


def _build_protected_cloud_mount(
    row: dict[str, Any], *, thread_id: str
) -> Optional[dict[str, Any]]:
    """Build the RO-lower + capture-overlay cloud_mount payload from an active
    ``cloud_ro_mounts`` row (design §3.1). The read-only lower uses the
    per-mount READER credential — never agent-service — and its transport is
    the provider adapter's. Returns None unless the row is an active grant of
    a provider that offers the protected level."""
    adapter = provider_adapter(row.get("backend")) if row else None
    if (
        not row
        or row.get("status") != "active"
        or not provider_offers(row.get("backend"), PROTECTED_PROJECT_FOLDER)
        or not isinstance(adapter, SupportsProtectedLower)
    ):
        return None
    lower_transport = adapter.protected_lower_transport(row)
    return {
        "version": 1,
        "driver": "rclone",
        "cloud_root": "/cloud",
        "workspace_entry": "cloud",
        "protected": True,
        "required": True,
        # The overlay manager owns workspace/cloud -> merged; the rclone manager
        # must NOT install its own workspace/cloud -> lower symlink.
        "skip_workspace_links": True,
        "overlay": {
            "lower": _PROTECTED_LOWER_TARGET,
            "upper": _PROTECTED_OVERLAY_UPPER,
            "work": _PROTECTED_OVERLAY_WORK,
            "merged": _PROTECTED_OVERLAY_MERGED,
            "quota_bytes": _PROTECTED_UPPERDIR_QUOTA_BYTES,
        },
        "fallback": False,
        "mounts": [
            {
                "mount_id": f"protected-{thread_id}",
                "mount_kind": "protected_lower",
                "target_path": _PROTECTED_LOWER_TARGET,
                "workspace_name": "lower",
                "access": "read_only",
                **lower_transport,
                "cache": {
                    "vfs_cache_mode": "full",
                    "vfs_cache_max_size": "10G",
                    "vfs_cache_max_age": "24h",
                    "dir_cache_time": "5m",
                    "poll_interval": "1m",
                    "vfs_read_chunk_size": "16M",
                    "vfs_read_chunk_size_limit": "128M",
                    "hard_cache_limit": "20G",
                },
            }
        ],
    }


@dataclass(frozen=True)
class LiveMountSet:
    """The ordinary (non-protected) mounts a thread gets, credentials included.

    ``excluded`` names every ``thread_mounts`` row the all-or-fallback rule
    left out, with a closed reason: ``unbuildable`` (``detail`` says why) for
    the row that could not be built, ``set_fallback`` for the rows dropped
    with it. Main-cloud slice 3 replaces the rule with per-connector binding.
    """

    mounts: list[dict[str, Any]]
    fallback: bool
    excluded: list[dict[str, Any]]


async def _resolve_live_mount_set(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
) -> LiveMountSet:
    """Every requested ``thread_mounts`` row, or the session folder instead."""
    rows = [
        row
        for row in (mount_rows or [])
        if row.get("backend_id") and row.get("cloud_handle")
    ]
    used_names: set[str] = set()
    mounted_rows: list[dict[str, Any]] = []
    failures: list[str] = []
    failed_row: dict[str, Any] | None = None
    for row in rows:
        workspace_name = _cloud_mount_name(row, used_names, dependencies=dependencies)
        mounted = await _build_rclone_mount_from_row(
            row,
            workspace_name=workspace_name,
            runtime_is_vm=runtime_is_vm,
            dependencies=dependencies,
            failures=failures,
        )
        if mounted is None:
            mounted_rows = []
            failed_row = row
            break
        mounted_rows.append(mounted)

    fallback = False
    mounts_out = mounted_rows
    excluded: list[dict[str, Any]] = []
    if not mounts_out or len(mounts_out) != len(rows):
        session_mount = await _build_rclone_session_mount(
            thread, runtime_is_vm=runtime_is_vm, dependencies=dependencies
        )
        if session_mount:
            mounts_out = [session_mount]
            fallback = bool(rows)
        for row in rows:
            entry: dict[str, Any] = {
                "source_ref": str(row.get("id") or row.get("source_ref") or ""),
                "mount_kind": str(row.get("mount_kind") or "project"),
                "reason": "set_fallback",
            }
            if row is failed_row:
                entry["reason"] = "unbuildable"
                entry["detail"] = failures[-1] if failures else "error"
            excluded.append(entry)
    return LiveMountSet(mounts=mounts_out, fallback=fallback, excluded=excluded)


async def _resolve_protected_mount(
    thread: dict[str, Any],
    *,
    metadata: dict[str, Any],
    terminal_retirement_token: int | None = None,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    """A protected-marked thread's payload (reader-grant lower + overlay), or
    ``None``: never a live mount."""
    row = await _resolve_protected_grant(
        thread,
        metadata=metadata,
        terminal_retirement_token=terminal_retirement_token,
        dependencies=dependencies,
    )
    if row is None:
        return None
    return _build_protected_cloud_mount(row, thread_id=str(thread.get("id")))


async def _resolve_protected_grant(
    thread: dict[str, Any],
    *,
    metadata: dict[str, Any],
    terminal_retirement_token: int | None = None,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    """A protected-marked thread's ``cloud_ro_mounts`` row, or ``None`` when
    the thread may have no protected mount. Runtime readiness is the
    caller's; the in-pod plane's planner (``cloud_mount_plan``) asks at Pod
    creation, when the workspace is not ready yet, and awaits the same
    engage task an attach would."""
    protected_marker = protected_cloud_marker_state(metadata)
    if protected_marker == "malformed":
        logger.warning(
            "Thread %s: malformed protected_cloud marker; refusing cloud mount.",
            thread.get("id"),
        )
        return None
    if not dependencies.is_protected_cloud_mode_enabled():
        logger.warning(
            "Thread %s: protected_cloud marker present but "
            "PROTECTED_CLOUD_MODE_ENABLED is off; refusing any cloud mount.",
            thread.get("id"),
        )
        return None
    vm_ctx = metadata.get("vm") or {}
    if vm_ctx.get("status") == "ready" and vm_ctx.get("ssh_host"):
        logger.warning(
            "Thread %s: protected cloud mode not supported on VM tier; no mount.",
            thread.get("id"),
        )
        return None
    tid = str(thread.get("id"))
    if terminal_retirement_token is not None:
        # End must never restart or wait for an engage task. It only
        # reconstructs a grant that the exact retiring runtime could
        # already have mounted; absent/non-active rows correctly leave no
        # payload for the terminal zero scan to accept.
        return await dependencies.store.get_ro_mount_by_thread(tid)
    runtime_authority = thread_runtime_authority(thread)
    if runtime_authority is None:
        logger.warning(
            "Thread %s: protected mount requested without open runtime "
            "generation authority; refusing.",
            tid,
        )
        return None
    engage_task_key = (tid, runtime_authority.generation)
    row = await dependencies.store.get_ro_mount_by_thread(tid)
    if row is None:
        # F-I1: engage-vs-attach race. Create-time engage is
        # fire-and-forget, so an early attach (or an idle-pool resume
        # that lands fast) can beat it here. Prefer awaiting the SAME
        # in-flight task (bounded) over guessing with a bare poll; only
        # fall back to polling when no task is registered for this
        # thread (e.g. a second replica handled create — HA) AND no
        # terminal error is already recorded (a refusal/error means the
        # task already ran to completion with nothing to wait for).
        task = dependencies.cloud_tasks.protected_engage_get(engage_task_key)
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=30)
            except Exception as e:
                logger.warning(
                    "Thread %s: awaiting in-flight protected engage "
                    "task failed/timed out: %s",
                    tid,
                    e,
                )
            row = await dependencies.store.get_ro_mount_by_thread(tid)
        elif not metadata.get("protected_cloud_error"):
            for _ in range(3):
                await asyncio.sleep(3)
                row = await dependencies.store.get_ro_mount_by_thread(tid)
                if row is not None:
                    break
    return row


async def _build_agent_cloud_mount(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    metadata: dict[str, Any],
    terminal_retirement_token: int | None = None,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    """Build the rclone ``cloud_mount`` payload for capable runtimes.

    v1 is all-or-fallback: if every requested thread_mount can be represented
    safely, mount those. If any requested mount is unsupported, mount the
    regular session folder instead when it exists. This prevents a default
    user-home row from falling back to the eager clone path. A terminal token
    selects the narrower End-only reconstruction gate; it does not widen
    normal runtime delivery.

    A Pod created with the in-pod plane (connector drivers D7) records its
    plan; its mounts come from its sidecars, so this returns the sidecar
    payload (``delivery: "sidecar"``, no credential) and never an
    in-workspace one, whatever the thread's rows say now: mounts are fixed
    when the Pod is created.
    """
    sidecar_plan = recorded_sidecar_plan(metadata)
    if sidecar_plan is not None:
        if terminal_retirement_token is None:
            workspace = metadata.get("workspace_container") or {}
            ready = workspace.get("status") == "ready" and bool(
                workspace.get("pod_ip") or workspace.get("host")
            )
        else:
            ready = _runtime_supports_terminal_rclone_retirement(
                thread,
                metadata,
                terminal_token=terminal_retirement_token,
                dependencies=dependencies,
            )
        return agent_payload(sidecar_plan) if ready else None
    if terminal_retirement_token is None:
        runtime_supported = _runtime_supports_rclone_mount(
            metadata, dependencies=dependencies
        )
        if runtime_supported and not _vm_runtime_ready(metadata):
            runtime_supported = not await container_denies_fuse(
                dependencies.store, thread
            )
    else:
        runtime_supported = _runtime_supports_terminal_rclone_retirement(
            thread,
            metadata,
            terminal_token=terminal_retirement_token,
            dependencies=dependencies,
        )
    if not runtime_supported:
        return None

    # Protected cloud mode: the marker ALONE routes a thread into this branch —
    # never gate the branch on the feature flag, or a protected-marked thread
    # served while the flag is OFF would fall through to the LIVE builders with
    # agent-service credentials (B8 review finding; violates the fail-closed
    # invariant). Flag off => protected threads get NO cloud, not live cloud.
    if protected_cloud_marker_state(metadata) != "off":
        return await _resolve_protected_mount(
            thread,
            metadata=metadata,
            terminal_retirement_token=terminal_retirement_token,
            dependencies=dependencies,
        )

    # A cross-cluster VM runtime needs the public WebDAV URL (it can't reach the
    # internal service DNS) and defaults to a read-only mount (root tier). A
    # same-cluster workspace pod keeps the internal URL + read-write.
    vm_ctx = metadata.get("vm") or {}
    runtime_is_vm = vm_ctx.get("status") == "ready" and bool(vm_ctx.get("ssh_host"))

    live = await _resolve_live_mount_set(
        thread,
        mount_rows=mount_rows,
        runtime_is_vm=runtime_is_vm,
        dependencies=dependencies,
    )
    if not live.mounts:
        return None

    return {
        "version": 1,
        "driver": "rclone",
        "cloud_root": "/cloud",
        "workspace_entry": "cloud",
        "fallback": live.fallback,
        "required": False,
        "mounts": live.mounts,
    }
