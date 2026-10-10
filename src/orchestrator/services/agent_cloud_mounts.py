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
* **Per connector, never all-or-fallback** (main cloud slice 3). A
  session mounts the cloud connectors it has attached, each on its own; one
  that cannot be mounted is named with a closed reason and nothing takes its
  place (``cloud_connector_mounts``). Only End's reconstruction of a session
  the old rule mounted still builds the old set, to adopt its residents.

Collaborators arrive through :class:`AgentCloudMountDependencies`, rebuilt per
invocation by the application rather than captured at import: the main-cloud
router and the postgres pool are both rebound during ``lifespan``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Optional

from orchestrator.services.cloud import (
    PROTECTED_PROJECT_FOLDER,
    CloudBackendError,
    CloudMountSubject,
    ProjectFolderHandle,
    SessionFolderHandle,
    SupportsFolderGrant,
    SupportsProtectedLower,
    SupportsRcloneMount,
    provider_adapter,
    provider_offers,
)
from orchestrator.services.cloud import folder_grants
from orchestrator.services.cloud.user_root_grant import SupportsUserRootGrant
from orchestrator.services.cloud.folder_grants import (
    KIND_FOR_ACCESS,
    FolderGrantTarget,
    FolderGrantUnavailable,
)
from orchestrator.services.cloud_connector_mounts import (
    FOLDER_KIND_BINDERS,
    FolderBindRequest,
    SelectedCloudConnector,
    excluded_entry,
    outbox_attached,
    outbox_record,
    selected_cloud_connectors,
    selected_ids,
    uses_connector_rule,
)
from orchestrator.services.cloud_mount_sidecar import (
    agent_payload,
    recorded_sidecar_plan,
)
from orchestrator.services.connector_drivers.cloud_folder import (
    CloudFolderMount,
    CloudFolderUnavailable,
)
from orchestrator.services.connector_drivers.cloud_outbox import (
    WORKSPACE_NAME as OUTBOX_WORKSPACE_NAME,
)
from orchestrator.services.connector_drivers.cloud_outbox import (
    CloudOutboxDriver,
    CloudOutboxMount,
    CloudOutboxUnavailable,
)
from orchestrator.services.sandbox_workspace_settings import container_denies_fuse
from orchestrator.services.session_runtime_admission import (
    protected_cloud_marker_state,
    thread_runtime_authority,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from shared.connectors.contract import AccessContext
from shared.connectors.platform import native_project

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
    """A browser URL for a thread's session outbox folder, or ``None``.

    The Cockpit's "Cloud" button (``thread.cloud_session_url ||
    thread.nc_session_folder``) opens it. A session has one only while its
    outbox is attached (``outbox_attached``; a session under the old rule
    whenever it has a session folder). No other folder stands in for it:
    the old ``project_default`` fallback went with the user-home rows
    (main cloud slices 3 and 4). ``mount_rows`` is accepted for the callers
    that still pass it.
    """
    del mount_rows
    handle_str = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    if not handle_str or not outbox_attached(thread_metadata_object(thread)):
        return None
    backend = dependencies.cloud_router.for_thread_optional(thread)
    if backend is None or not backend.is_initialized:
        return None
    try:
        handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
        return backend.get_session_folder_browser_url(handle)
    except Exception:
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

    ``session_folder`` is the session's outbox folder, sent only while the
    outbox is attached (``outbox_attached``; a session under the old rule
    whenever it has one). ``mounts`` are the project / user-home / repo
    mounts derived from ``thread_mounts``. Returns ``None`` when neither a
    session folder nor any mount could be resolved — in that case there's
    nothing for the agent to sync. Never logs auth payloads.
    """
    # ---- the session's outbox folder
    session_folder_cfg: Optional[dict[str, Any]] = None
    handle_str = thread.get("main_cloud_session_handle") or thread.get(
        "nc_session_folder"
    )
    if handle_str and outbox_attached(thread_metadata_object(thread)):
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
        if row.get("mount_kind") == "project" and isinstance(
            backend, SupportsFolderGrant
        ):
            # A project folder of a provider with per-folder grants is reached
            # only as its grant's principal, which this sync path does not
            # carry: never as the service account (main cloud slice 7).
            logger.info(
                "Thread mount %s: a project folder is not synced as the service "
                "account; skipped",
                row.get("id") or row.get("source_ref"),
            )
            continue
        if row.get("mount_kind") == "project_default" and isinstance(
            backend, SupportsUserRootGrant
        ):
            # A user's root of a provider with user roots is the user's own
            # connector, mounted with their own credential (main cloud slice
            # 4): never synced as the service account.
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
    access: str | None = None,
    subject: CloudMountSubject | None = None,
    legacy: bool = False,
    dependencies: AgentCloudMountDependencies,
    failures: list[str] | None = None,
) -> Optional[dict[str, Any]]:
    """One row's rclone mount, or ``None``; ``failures`` gets why, as a
    closed code (``no_transport``, ``backend_unavailable``, a
    ``CloudBackendErrorKind`` value, or ``error``), never the error text.

    ``access`` is the level a connector's driver resolved, and ``subject``
    the principal a project folder is mounted as (its per-folder grant's).
    ``legacy`` is End's reconstruction of a mount the old rule made in the
    workspace as the service account, at the level the old rule gave it.
    No level, no mount: a tier never picks one."""

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
        if subject is None:
            subject = CloudMountSubject(
                user_sub=row.get("target_user_sub"),
                username=handle.vendor_meta.get("username"),
                legacy_service_account=legacy,
            )
        target_path = f"/cloud/{workspace_name}"
        if access not in ("read_only", "read_write"):
            refused("no_access")
            return None
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


async def _legacy_session_folder_mount(
    thread: dict[str, Any],
    *,
    runtime_is_vm: bool = False,
    dependencies: AgentCloudMountDependencies,
) -> Optional[dict[str, Any]]:
    """The old rule's substitute: the session folder as ``cloud/home``.

    Only End's reconstruction of a session the old rule mounted builds it
    (:func:`_legacy_live_mount_set`), byte for byte as that rule did, so the
    resident rclone process can be adopted and removed. No delivery mounts
    it any more: the session folder is the outbox connector's.
    """
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

    ``excluded`` names every attached cloud connector that mounts nothing,
    with a closed reason (``cloud_connector_mounts.EXCLUSION_REASONS``) and
    the connector's ``title``; nothing took its place. ``fallback`` is the
    old rule's, set only by End's reconstruction of a session it mounted
    (:func:`_legacy_live_mount_set`).
    """

    mounts: list[dict[str, Any]]
    excluded: list[dict[str, Any]] = field(default_factory=list)
    fallback: bool = False


def _unique_name(base: str, used: set[str]) -> str:
    name = base
    suffix = 2
    while name in used:
        name = f"{base}-{suffix}"
        suffix += 1
    used.add(name)
    return name


def _project_scope(
    thread: dict[str, Any], mount_rows: list[dict[str, Any]] | None
) -> list[str]:
    from orchestrator.services.thread_mount_rows import durable_project_ids

    return durable_project_ids(thread, legacy_mounts=mount_rows)


async def _resolve_live_mount_set(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
) -> LiveMountSet:
    """Each cloud connector the thread has attached, bound on its own.

    The selection is the thread's (``metadata.datasource_ids``); a connector
    its owner may no longer attach is ``not_eligible``. ``mount_rows`` only
    gives a legacy multi-project session its project scope; no row is
    mounted from them any more. A selection that cannot be read mounts
    nothing, as a mount that fails never holds the session back.
    """
    try:
        selected = await selected_cloud_connectors(
            thread,
            datasource_ids=selected_ids(thread_metadata_object(thread)),
            project_ids=_project_scope(thread, mount_rows),
            store=dependencies.store,
        )
    except Exception:
        logger.warning(
            "Thread %s: could not read its cloud connectors; no cloud folder "
            "is mounted now",
            thread.get("id"),
            exc_info=True,
        )
        return LiveMountSet(mounts=[])
    return await _bind_cloud_connectors(
        thread, selected, runtime_is_vm=runtime_is_vm, dependencies=dependencies
    )


def _access_context(thread: dict[str, Any], *, runtime_is_vm: bool) -> AccessContext:
    # An ordinary session's folders never bind at the reviewed level: only a
    # protected session has the review lane, and it takes its own path. A
    # link default of ``protected`` is lowered (read-only where offered),
    # never mounted live at it.
    return AccessContext(
        workspace_backend="vm" if runtime_is_vm else "sandbox",
        protected_mode_enabled=False,
        execution="session",
        main_cloud_backend=str(thread.get("main_cloud_backend") or "") or None,
    )


async def _bind_cloud_connectors(
    thread: dict[str, Any],
    selected: list[SelectedCloudConnector],
    *,
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
) -> LiveMountSet:
    """``selected`` mounted one by one; the outbox first, so it keeps
    ``cloud/outbox`` whatever a project is called."""
    ctx = _access_context(thread, runtime_is_vm=runtime_is_vm)
    used: set[str] = set()
    mounts: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    ordered = sorted(
        selected,
        key=lambda connector: not isinstance(connector.driver, CloudOutboxDriver),
    )
    for connector in ordered:
        if not connector.eligible:
            built: dict[str, Any] = excluded_entry(
                connector,
                "not_eligible",
                detail="This session's owner may no longer attach this folder",
            )
        elif isinstance(connector.driver, CloudOutboxDriver):
            built = await _bind_outbox(
                thread, connector, ctx=ctx, used=used, dependencies=dependencies
            )
        else:
            built = await _bind_folder(
                thread,
                connector,
                ctx=ctx,
                used=used,
                runtime_is_vm=runtime_is_vm,
                dependencies=dependencies,
            )
        if "mount_id" in built:
            mounts.append(built)
        else:
            excluded.append({**built, "title": str(connector.row.get("name") or "")})
    return LiveMountSet(mounts=mounts, excluded=excluded)


def _folder_binding(
    connector: SelectedCloudConnector, *, ctx: AccessContext
) -> CloudFolderMount | CloudFolderUnavailable:
    """A ``cloud_folder`` connector's binding: at the level the session
    resolved for it, else its project link's default, within what its owner
    may have now (``ceiling``). Its mount and its grant follow the same."""
    return connector.driver.mount_binding(
        connector.row,
        ctx=ctx,
        requested=connector.requested,
        ceiling=connector.ceiling,
    )


async def _bind_folder(
    thread: dict[str, Any],
    connector: SelectedCloudConnector,
    *,
    ctx: AccessContext,
    used: set[str],
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
) -> dict[str, Any]:
    """A ``cloud_folder`` connector's mount, or its exclusion: at the level
    the session resolved for it, else its project link's default, within
    what its owner may have now (``ceiling``)."""
    binding = _folder_binding(connector, ctx=ctx)
    if isinstance(binding, CloudFolderUnavailable):
        return excluded_entry(connector, binding.reason, detail=binding.detail)
    level = binding.access
    binder = FOLDER_KIND_BINDERS.get(binding.folder_kind)
    if binder is None:
        return excluded_entry(
            connector,
            "config_invalid",
            detail="SRW mounts no folder of this kind",
            level=level,
        )
    bound = await binder(
        FolderBindRequest(
            thread=thread,
            row=connector.row,
            binding=binding,
            store=dependencies.store,
            slugify=dependencies.slugify_mount_name,
            cloud_router=dependencies.cloud_router,
        )
    )
    if isinstance(bound, CloudFolderUnavailable):
        return excluded_entry(connector, bound.reason, detail=bound.detail, level=level)
    granted = await _folder_grant_subject(
        thread, bound.row, binding, dependencies=dependencies
    )
    if isinstance(granted, CloudFolderUnavailable):
        return excluded_entry(
            connector,
            granted.reason,
            name=bound.name,
            detail=granted.detail,
            level=level,
        )
    name = _unique_name(bound.name, used)
    failures: list[str] = []
    built = await _build_rclone_mount_from_row(
        bound.row,
        workspace_name=name,
        runtime_is_vm=runtime_is_vm,
        access=binding.access,
        subject=granted if granted is not None else bound.subject,
        dependencies=dependencies,
        failures=failures,
    )
    if built is None:
        return excluded_entry(
            connector,
            "unbuildable",
            name=name,
            detail=failures[-1] if failures else "error",
            level=level,
        )
    return built


def _grant_backend(
    row: dict[str, Any], *, dependencies: AgentCloudMountDependencies
) -> Any:
    """The folder's installation when its provider has per-folder grants,
    else ``None`` (its mount, or why not, is the rclone builder's)."""
    try:
        backend = dependencies.cloud_router.for_backend_instance(
            str(row.get("backend_instance_id") or ""),
            expected_backend_id=str(row.get("backend_id") or ""),
        )
    except Exception:
        return None
    return backend if isinstance(backend, SupportsFolderGrant) else None


def _grant_slot(
    thread: dict[str, Any],
    row: dict[str, Any],
    binding: CloudFolderMount,
    *,
    dependencies: AgentCloudMountDependencies,
) -> tuple[FolderGrantTarget, str] | CloudFolderUnavailable | None:
    """Which grant a bound project folder is mounted with: its target and
    kind (the level's), ``None`` for a provider without grants, or why it
    cannot be mounted with one."""
    if (
        binding.folder_kind != "project"
        or _grant_backend(row, dependencies=dependencies) is None
    ):
        return None
    kind = KIND_FOR_ACCESS.get(binding.access)
    target = FolderGrantTarget.from_mount_row(row, project_id=binding.project_id)
    if kind is None or target is None or not thread.get("user_id"):
        return CloudFolderUnavailable(
            binding.connector_id,
            "grant_unavailable",
            "no_grant_for_level" if kind is None else "folder_invalid",
        )
    return target, kind


async def _folder_grant_subject(
    thread: dict[str, Any],
    row: dict[str, Any],
    binding: CloudFolderMount,
    *,
    dependencies: AgentCloudMountDependencies,
) -> CloudMountSubject | CloudFolderUnavailable | None:
    """The principal a bound project folder is mounted as: its active
    per-folder grant for the session's owner at the bound level (a reader
    for ``read_only``, a writer for ``read_write``), found, never minted
    here (the planner may hold the thread's lock; the grants were prepared
    before, :func:`prepare_folder_grants`). Without one the folder is left
    out (``grant_unavailable``, ``reader_unavailable`` for a reader, its
    ``detail`` the closed code of a failed mint or ``not_ready``): never
    the service account in its place. ``None`` for a provider without
    grants, whose own credential the adapter uses."""
    slot = _grant_slot(thread, row, binding, dependencies=dependencies)
    if slot is None or isinstance(slot, CloudFolderUnavailable):
        return slot
    target, kind = slot
    store = dependencies.store
    user_id = str(thread["user_id"])
    try:
        grant = await folder_grants.lookup_active_folder_grant(
            store, target, user_id=user_id, kind=kind
        )
        failure = (
            None
            if grant is not None
            else await store.latest_cloud_folder_grant_failure(
                target.datasource_id, user_id, kind
            )
        )
    except Exception:
        logger.warning(
            "Thread %s: could not read the grant of cloud folder %s",
            thread.get("id"),
            binding.connector_id,
            exc_info=True,
        )
        grant, failure = None, "error"
    if grant is None:
        return CloudFolderUnavailable(
            binding.connector_id,
            "reader_unavailable" if kind == "reader" else "grant_unavailable",
            failure if failure in folder_grants.FAILURE_CODES else "not_ready",
        )
    return grant.subject()


#: How long one session's grant preparation may take in all (a reader's
#: read-only probe takes seconds; a writer's mint about one).
GRANT_PREPARE_SECONDS = 60.0


async def prepare_folder_grants(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
    timeout_s: float = GRANT_PREPARE_SECONDS,
) -> None:
    """Mint the per-folder grants a session's project folders are mounted
    with, before the mount is built (never under the thread's advisory
    lock: the provider is called and a reader is probed). Each attached
    project folder at the level it binds at on this tier, for the
    session's owner; one already active is only looked up. Bounded and
    best effort: a grant that is not ready leaves its folder out, with the
    reason, and the next preparation tries again. Never raises."""
    metadata = thread_metadata_object(thread)
    if protected_cloud_marker_state(metadata) != "off" or not thread.get("user_id"):
        return
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    try:
        selected = await selected_cloud_connectors(
            thread,
            datasource_ids=selected_ids(metadata),
            project_ids=_project_scope(thread, mount_rows),
            store=dependencies.store,
        )
        ctx = _access_context(thread, runtime_is_vm=runtime_is_vm)
        for connector in selected:
            if not connector.eligible or isinstance(
                connector.driver, CloudOutboxDriver
            ):
                continue
            binding = _folder_binding(connector, ctx=ctx)
            if not isinstance(binding, CloudFolderMount):
                continue
            binder = FOLDER_KIND_BINDERS.get(binding.folder_kind)
            if binding.folder_kind != "project" or binder is None:
                continue
            bound = await binder(
                FolderBindRequest(
                    thread=thread,
                    row=connector.row,
                    binding=binding,
                    store=dependencies.store,
                    slugify=dependencies.slugify_mount_name,
                )
            )
            if isinstance(bound, CloudFolderUnavailable):
                continue
            slot = _grant_slot(thread, bound.row, binding, dependencies=dependencies)
            if slot is None or isinstance(slot, CloudFolderUnavailable):
                continue
            target, kind = slot
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.info(
                    "Thread %s: out of time preparing its cloud folders' grants",
                    thread.get("id"),
                )
                return
            # Shielded: past the budget the mint goes on in the background
            # (a reader's probe takes a while) and the next preparation finds
            # it active.
            mint = folder_grants.start_folder_grant(
                dependencies.store,
                dependencies.cloud_router,
                target,
                user_id=str(thread["user_id"]),
                kind=kind,
            )
            try:
                await asyncio.wait_for(asyncio.shield(mint), timeout=remaining)
            except FolderGrantUnavailable as exc:
                logger.info(
                    "Thread %s: no %s grant for cloud folder %s (%s)",
                    thread.get("id"),
                    kind,
                    binding.connector_id,
                    exc.code,
                )
            except (asyncio.TimeoutError, TimeoutError):
                logger.info(
                    "Thread %s: the %s grant for cloud folder %s is not ready yet",
                    thread.get("id"),
                    kind,
                    binding.connector_id,
                )
                return
    except Exception:
        logger.warning(
            "Thread %s: its cloud folders' grants were not prepared",
            thread.get("id"),
            exc_info=True,
        )


async def prepare_thread_folder_grants(
    thread_id: str, *, dependencies: AgentCloudMountDependencies
) -> None:
    """:func:`prepare_folder_grants` for a session whose folders its
    workspace mounts itself (a VM, an officer, a Pod without the in-pod
    plane): the attach path's preparation, before its locked response. A
    Pod whose sidecars mount its folders needs none here (its planner
    prepared them). Never raises."""
    try:
        thread = await dependencies.store.get_thread(thread_id)
        if not thread:
            return
        metadata = thread_metadata_object(thread)
        runtime_is_vm = _vm_runtime_ready(metadata)
        if recorded_sidecar_plan(metadata) is not None and not runtime_is_vm:
            return
        await prepare_folder_grants(
            thread,
            mount_rows=await dependencies.store.list_thread_mounts(thread_id),
            runtime_is_vm=runtime_is_vm,
            dependencies=dependencies,
        )
    except Exception:
        logger.warning(
            "Thread %s: its cloud folders' grants were not prepared",
            thread_id,
            exc_info=True,
        )


async def _bind_outbox(
    thread: dict[str, Any],
    connector: SelectedCloudConnector,
    *,
    ctx: AccessContext,
    used: set[str],
    dependencies: AgentCloudMountDependencies,
) -> dict[str, Any]:
    """The session's outbox mount at ``cloud/outbox``, or its exclusion.

    The folder is the one SRW created for the session
    (``main_cloud_session_handle``), on the main cloud stamped on it; a
    session whose folder SRW could not create says why
    (``cloud_outbox.reason``)."""
    binding = connector.driver.mount_binding(
        connector.row,
        ctx=ctx,
        owner_id=str(thread["user_id"]) if thread.get("user_id") else None,
        session_folder=thread.get("main_cloud_session_handle")
        or thread.get("nc_session_folder"),
        backend_instance_id=(
            str(thread["main_cloud_backend_instance_id"])
            if thread.get("main_cloud_backend_instance_id")
            else None
        ),
        shared=bool(thread.get("main_cloud_share_handle")),
    )
    if isinstance(binding, CloudOutboxUnavailable):
        detail = binding.detail
        if binding.reason == "no_session_folder":
            recorded = outbox_record(thread_metadata_object(thread)).get("reason")
            detail = str(recorded) if recorded else "not_created"
        return excluded_entry(
            connector,
            binding.reason,
            name=OUTBOX_WORKSPACE_NAME,
            detail=detail,
        )
    name = _unique_name(binding.name, used)
    failures: list[str] = []
    built = await _build_rclone_outbox_mount(
        thread,
        binding,
        workspace_name=name,
        dependencies=dependencies,
        failures=failures,
    )
    if built is None:
        return excluded_entry(
            connector,
            "unbuildable",
            name=name,
            detail=failures[-1] if failures else "error",
            level=binding.access,
        )
    return built


async def _build_rclone_outbox_mount(
    thread: dict[str, Any],
    outbox: CloudOutboxMount,
    *,
    workspace_name: str,
    dependencies: AgentCloudMountDependencies,
    failures: list[str],
) -> Optional[dict[str, Any]]:
    """The outbox's rclone mount, or ``None`` with a closed code in
    ``failures``, as :func:`_build_rclone_mount_from_row` reports one."""
    backend = dependencies.cloud_router.for_thread_optional(thread)
    if (
        backend is None
        or not backend.is_initialized
        or not isinstance(backend, SupportsRcloneMount)
        or backend.backend_id != outbox.backend_id
    ):
        failures.append("backend_unavailable")
        return None
    target_path = f"/cloud/{workspace_name}"
    try:
        handle = SessionFolderHandle.from_db(
            outbox.folder_handle, backend=backend.backend_id
        )
        spec = await backend.build_rclone_mount_spec(
            handle=handle,
            mount_kind="session_folder",
            target_path=target_path,
            access=outbox.access,
            subject=None,
            prefer_public_url=False,
        )
    except CloudBackendError as e:
        logger.info(
            "Thread %s: its outbox cannot use rclone (%s)",
            thread.get("id"),
            e.kind.value,
        )
        failures.append(str(e.kind.value))
        return None
    except Exception as e:
        logger.warning(
            "Thread %s: failed to build its outbox's rclone spec: %s",
            thread.get("id"),
            e,
        )
        failures.append("error")
        return None
    return {
        "mount_id": outbox.connector_id,
        "mount_kind": "cloud_outbox",
        "backend": backend.backend_id,
        "target_path": target_path,
        "workspace_name": workspace_name,
        "access": outbox.access,
        "source_ref": outbox.connector_id,
        **spec.to_payload(),
    }


async def _convert_once(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    dependencies: AgentCloudMountDependencies,
) -> dict[str, Any] | None:
    """A session created before cloud connectors, converted at its first
    in-workspace delivery (a workspace without a recorded plan; a new Pod
    converts in its planner). Never fails the delivery."""
    from orchestrator.services.session_cloud_preparation import (
        convert_to_connector_rule,
    )

    try:
        return await convert_to_connector_rule(
            thread, mount_rows=mount_rows, store=dependencies.store
        )
    except Exception:
        logger.warning(
            "Thread %s: its cloud folders did not convert; the next delivery "
            "retries it",
            thread.get("id"),
            exc_info=True,
        )
        return None


async def beside_protected_exclusions(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    dependencies: AgentCloudMountDependencies,
) -> list[dict[str, str]]:
    """Every attached cloud connector a protected session leaves out.

    A protected session mounts its protected folder alone (its
    ``thread_protected_sources`` row: the folder its connector binds at
    protected; for one release an older session's ``thread_mounts`` row);
    every other cloud connector it has attached, its outbox included, is
    named ``beside_protected`` rather than dropped silently. ``mount_rows``
    give a legacy multi-project session its project scope.
    """
    from orchestrator.services.cloud_staging import select_protected_mount

    try:
        protected = (
            select_protected_mount(
                await dependencies.store.list_protected_selection_rows(
                    str(thread.get("id"))
                )
            )
            or {}
        )
        selected = await selected_cloud_connectors(
            thread,
            datasource_ids=selected_ids(thread_metadata_object(thread)),
            project_ids=_project_scope(thread, mount_rows),
            store=dependencies.store,
        )
    except Exception:
        # Naming them is the report; it never holds the protected mount up.
        logger.warning(
            "Thread %s: could not list the cloud folders beside its protected one",
            thread.get("id"),
            exc_info=True,
        )
        return []
    protected_connector = str(protected.get("datasource_id") or "")
    protected_project = str(protected.get("source_ref") or "")

    def is_protected(connector: SelectedCloudConnector) -> bool:
        # A connector-sourced folder names its connector; one copied from
        # thread_mounts names only its project.
        if protected_connector:
            return connector.connector_id == protected_connector
        return bool(protected_project) and (
            native_project(connector.row) == protected_project
        )

    return [
        {
            **excluded_entry(connector, "beside_protected"),
            "title": str(connector.row.get("name") or ""),
        }
        for connector in selected
        if not is_protected(connector)
    ]


async def sync_delivery_exclusions(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    dependencies: AgentCloudMountDependencies,
) -> list[dict[str, str]]:
    """Every attached cloud folder a deployment that syncs files instead of
    mounting them (``CLOUD_WORKSPACE_DRIVER=sync``) does not deliver.

    Under that driver only the old rule's sync runs: the session folder,
    and an older session's ``thread_mounts`` rows, which keep what the sync
    did for them. A session under the connector rule has no rows, so each
    ``cloud_folder`` connector it attached (a project's folder, the owner's
    root) is named ``delivery_unsupported`` instead of silently missing.
    Its outbox is the session folder the sync already carries, so it is not
    named. A protected session has no sync at all and says so its own way.
    """
    metadata = thread_metadata_object(thread)
    if (
        not uses_connector_rule(metadata)
        or protected_cloud_marker_state(metadata) != "off"
    ):
        return []
    try:
        selected = await selected_cloud_connectors(
            thread,
            datasource_ids=selected_ids(metadata),
            project_ids=_project_scope(thread, mount_rows),
            store=dependencies.store,
        )
    except Exception:
        # Naming them is the report; it never holds the workspace up.
        logger.warning(
            "Thread %s: could not list the cloud folders its sync does not deliver",
            thread.get("id"),
            exc_info=True,
        )
        return []
    return [
        {
            **excluded_entry(connector, "delivery_unsupported"),
            "title": str(connector.row.get("name") or ""),
        }
        for connector in selected
        if not isinstance(connector.driver, CloudOutboxDriver)
    ]


async def _legacy_live_mount_set(
    thread: dict[str, Any],
    *,
    mount_rows: list[dict[str, Any]] | None,
    runtime_is_vm: bool,
    dependencies: AgentCloudMountDependencies,
) -> LiveMountSet:
    """The old rule, for End's reconstruction only: every requested
    ``thread_mounts`` row, or the session folder instead.

    A session the old rule mounted in its workspace (no recorded plan, no
    ``cloud_mount_rule`` stamp) has resident rclone processes whose
    non-secret spec End must rebuild byte for byte to adopt and remove them
    (``_runtime_supports_terminal_rclone_retirement``). No delivery uses it.
    """
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
            # The old rule's level (a root VM got its folders read-only, an
            # rclone flag only): what the resident process was mounted with.
            access="read_only" if runtime_is_vm else "read_write",
            legacy=True,
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
        session_mount = await _legacy_session_folder_mount(
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

    Each attached cloud connector mounts on its own
    (:func:`_resolve_live_mount_set`); the ones that cannot are named in
    ``excluded``. A session created before the connector rule converts once
    here (``session_cloud_preparation.convert_to_connector_rule``) when its
    workspace has no recorded plan. A terminal token selects the narrower
    End-only reconstruction gate; it does not widen normal runtime delivery,
    and for a session the old rule mounted (no ``cloud_mount_rule`` stamp)
    End rebuilds the old rule's set (:func:`_legacy_live_mount_set`), which
    its resident processes were mounted from.

    A Pod created with the in-pod plane (connector drivers D7) records its
    plan; its mounts come from its sidecars, so this returns the sidecar
    payload (``delivery: "sidecar"``, no credential) and never an
    in-workspace one, whatever the thread's rows say now: mounts are fixed
    when the Pod is created.
    """
    sidecar_plan = recorded_sidecar_plan(metadata)
    # A sandbox upgraded to a VM keeps the Pod's record a while; the VM's
    # folders are its own (the in-workspace path), never the old Pod's.
    if sidecar_plan is not None and not _vm_runtime_ready(metadata):
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
        if not ready:
            return None
        payload = agent_payload(sidecar_plan, status=metadata.get("cloud_mount_status"))
        if not payload.get("protected") and any(
            mount.get("mount_kind") == "cloud_outbox"
            for mount in payload.get("mounts") or []
        ):
            # Whether the user sees the outbox yet: the share may land after
            # the Pod was planned (an unprotected payload is no attach
            # identity, so it may change between attaches).
            payload["outbox_shared"] = bool(thread.get("main_cloud_share_handle"))
        return payload
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
        payload = await _resolve_protected_mount(
            thread,
            metadata=metadata,
            terminal_retirement_token=terminal_retirement_token,
            dependencies=dependencies,
        )
        if payload is not None and terminal_retirement_token is None:
            # Its other folders are named, not dropped (the agent ignores
            # the key when it validates the protected contract).
            payload["excluded"] = await beside_protected_exclusions(
                {**thread, "metadata": metadata},
                mount_rows=mount_rows,
                dependencies=dependencies,
            )
        return payload

    # A cross-cluster VM runtime needs the public WebDAV URL (it can't reach the
    # internal service DNS). A connector's driver decides its level per tier.
    vm_ctx = metadata.get("vm") or {}
    runtime_is_vm = vm_ctx.get("status") == "ready" and bool(vm_ctx.get("ssh_host"))

    legacy = terminal_retirement_token is not None and not uses_connector_rule(metadata)
    if legacy:
        live = await _legacy_live_mount_set(
            thread,
            mount_rows=mount_rows,
            runtime_is_vm=runtime_is_vm,
            dependencies=dependencies,
        )
    else:
        if terminal_retirement_token is None and not uses_connector_rule(metadata):
            converted = await _convert_once(
                {**thread, "metadata": metadata},
                mount_rows=mount_rows,
                dependencies=dependencies,
            )
            if converted is not None:
                thread = converted
                metadata = thread_metadata_object(converted)
        live = await _resolve_live_mount_set(
            {**thread, "metadata": metadata},
            mount_rows=mount_rows,
            runtime_is_vm=runtime_is_vm,
            dependencies=dependencies,
        )
    if not live.mounts:
        return None

    payload: dict[str, Any] = {
        "version": 1,
        "driver": "rclone",
        "cloud_root": "/cloud",
        "workspace_entry": "cloud",
        "fallback": live.fallback,
        "required": False,
        "mounts": live.mounts,
    }
    if legacy:
        # The old rule's payload, exactly as it was delivered.
        return payload
    payload["excluded"] = live.excluded
    if any(mount.get("mount_kind") == "cloud_outbox" for mount in live.mounts):
        payload["outbox_shared"] = bool(thread.get("main_cloud_share_handle"))
    return payload
