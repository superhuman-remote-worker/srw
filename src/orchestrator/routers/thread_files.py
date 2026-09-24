"""HTTP adapters for a persistent thread's IDE status and workspace uploads.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane W).

The two file routes are lane-split, and the split is the contract:

* **Pinned** threads resolve the destination from a freshly re-read row and
  then prove it — a VM target through a durable remote-operation lease, a
  Kubernetes target through the captured-and-re-proved snapshot in
  :mod:`orchestrator.services.thread_files`.
* **Stateless** threads serialize the *entire* body materialization and remote
  write against End/Resume under the workspace ensure-lock. A marker-first
  upload never opens SFTP; a write-first End waits for the final exact-runtime
  proof and byte commit.

``get_thread_ide_status`` withholds a URL the proxy would refuse while keeping
the Gitea link, so the header always keeps one working way into the workspace,
and it never echoes an unvalidated repository name into that link.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile

from orchestrator.security.access import require_thread_owner
from orchestrator.services.gitea import GiteaPathError, validate_gitea_name
from orchestrator.services.ide_proxy import contain_ide_status_for
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.thread_files import (
    _prepare_pinned_k8s_thread_upload,
    _prepare_pinned_vm_thread_operation,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.session_retirement import stateless_stop_markers

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class ThreadFilesDependencies:
    """Per-app store, provisioners, backend policy readers and the owner gate."""

    store: Any
    container_provisioner: Any
    vm_provisioner: Any
    thread_workspace_backend: Callable[[Any], Any]
    require_stateless_workspace: Callable[[dict[str, Any]], str]
    require_thread_owner: Callable[..., Awaitable[Any]] = require_thread_owner
    vm_ide_transport: Any = None


def get_thread_files_dependencies(request: Request) -> ThreadFilesDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.thread_files_dependencies_factory()


def _vm_thread(thread: dict[str, Any]) -> bool:
    metadata = thread_metadata_object(thread)
    return (
        thread.get("execution_lane") == "pinned"
        and isinstance(metadata.get("vm"), dict)
        and bool(metadata["vm"])
    )


def _vm_thread_ide_url(thread_id: str, lease_id: str) -> str:
    base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085").rstrip("/")
    return (
        f"{base}/api/ide/{thread_id}/proxy/_vm/{lease_id}/"
        "?folder=/home/agent-host/workspace"
    )


@router.post("/api/persistent/threads/{thread_id}/ide")
async def start_thread_ide_session(
    thread_id: str,
    request: Request,
    *,
    dependencies: ThreadFilesDependencies = Depends(get_thread_files_dependencies),
) -> dict[str, Any]:
    """One explicit owner gesture admits a bounded pinned-VM IDE connection."""
    from orchestrator.services.vm_idle_access import VMIdleAccessStore
    from orchestrator.services.vm_ide_transport import VMIDEUnavailable, matches_admitted_runtime

    user, thread = await dependencies.require_thread_owner(
        request,
        dependencies.store,
        thread_id,
    )
    if not _vm_thread(thread):
        raise HTTPException(
            status_code=409, detail="VM IDE is not available for this session"
        )
    access = VMIdleAccessStore(dependencies.store)
    lease = await access.request(
        owner_kind="thread", owner_id=thread_id, kind="ide", user_id=str(user["id"])
    )
    if lease is None:
        raise HTTPException(status_code=409, detail="VM access authority changed")
    lease_id = str(lease["id"])
    current = await access.inspect_for_user(
        lease_id,
        owner_kind="thread",
        owner_id=thread_id,
        kind="ide",
        user_id=str(user["id"]),
    )
    if current is None:
        return {
            "status": "restoring",
            "code_server_url": None,
            "access_lease_id": lease_id,
            "estimated_seconds": 120,
        }
    if dependencies.vm_ide_transport is None:
        await access.close_for_user(
            lease_id,
            owner_kind="thread",
            owner_id=thread_id,
            kind="ide",
            user_id=str(user["id"]),
        )
        raise HTTPException(status_code=503, detail="VM IDE transport is unavailable")
    try:
        proof = await dependencies.vm_ide_transport.start_and_probe(
            thread_id, owner_kind="thread",
            expected_generation=lease["provision_generation"],
            expected_vm_uid=lease["vm_uid"],
        )
        if not matches_admitted_runtime(
            proof, lease["provision_generation"], lease["vm_uid"],
        ):
            raise VMIDEUnavailable("ide_runtime_changed")
    except VMIDEUnavailable as exc:
        await access.close_for_user(
            lease_id,
            owner_kind="thread",
            owner_id=thread_id,
            kind="ide",
            user_id=str(user["id"]),
        )
        return {"status": "unavailable", "code_server_url": None, "code": exc.code}
    if await access.inspect_for_user(
        lease_id, owner_kind="thread", owner_id=thread_id, kind="ide",
        user_id=str(user["id"]),
    ) is None:
        await access.close_for_user(
            lease_id, owner_kind="thread", owner_id=thread_id, kind="ide",
            user_id=str(user["id"]),
        )
        return {"status": "unavailable", "code_server_url": None,
                "code": "ide_runtime_changed"}
    return {
        "status": "active",
        "access_lease_id": lease_id,
        "code_server_url": _vm_thread_ide_url(thread_id, lease_id),
        "expires_at": lease["expires_at"].isoformat(),
    }


@router.delete("/api/persistent/threads/{thread_id}/ide")
async def stop_thread_ide_session(
    thread_id: str,
    request: Request,
    *,
    lease_id: str,
    dependencies: ThreadFilesDependencies = Depends(get_thread_files_dependencies),
) -> dict[str, str]:
    from orchestrator.services.vm_idle_access import VMIdleAccessStore

    user, _ = await dependencies.require_thread_owner(
        request,
        dependencies.store,
        thread_id,
    )
    if not await VMIdleAccessStore(dependencies.store).close_for_user(
        lease_id,
        owner_kind="thread",
        owner_id=thread_id,
        kind="ide",
        user_id=str(user["id"]),
    ):
        raise HTTPException(status_code=409, detail="VM IDE access changed")
    return {"status": "stopped"}


@router.get("/api/persistent/threads/{thread_id}/ide")
async def get_thread_ide_status(
    thread_id: str,
    request: Request,
    lease_id: str | None = None,
    *,
    dependencies: ThreadFilesDependencies = Depends(get_thread_files_dependencies),
) -> dict[str, Any]:
    """Get IDE session status for a persistent thread's workspace.

    Returns the workspace container or VM status with a code-server URL
    when the workspace is ready. The proxy path uses the thread_id in
    place of job_id: ``/api/ide/{thread_id}/proxy/``.
    """
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )

    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        import json as _json

        try:
            metadata = _json.loads(metadata)
        except (ValueError, TypeError):
            metadata = {}

    # Build Gitea web URL if repo exists
    ws_ctx = metadata.get("workspace_container", {})
    repo_name = ws_ctx.get("repo_name")
    gitea_url = None
    if repo_name:
        gitea_base = os.environ.get("GITEA_URL", "").rstrip("/")
        gitea_user = os.environ.get("GITEA_ADMIN_USER", "srw")
        try:
            repo_path = (
                f"{validate_gitea_name(gitea_user, kind='owner')}"
                f"/{validate_gitea_name(str(repo_name))}"
            )
        except GiteaPathError:
            # Job context is caller-writable; never echo an unvalidated
            # name into a link, even a display-only one.
            repo_path = None
        if gitea_base and repo_path:
            gitea_url = f"{gitea_base}/{repo_path}"

    if _vm_thread(thread):
        from orchestrator.services.vm_idle_access import VMIdleAccessStore
        from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore
        from orchestrator.services.vm_idle_public import read_vm_idle_states
        from orchestrator.services.vm_ide_transport import VMIDEUnavailable, matches_admitted_runtime

        states = await read_vm_idle_states(
            dependencies.store,
            owner_kind="thread",
            owner_ids=[thread_id],
        )
        lifecycle = {"workspace_lifecycle": states.get(thread_id)}
        active = await VMIdleLifecycleStore(dependencies.store).get_open_for_thread(
            thread_id
        )
        if active is not None:
            return {
                **lifecycle,
                "status": "restoring",
                "code_server_url": None,
                "gitea_url": gitea_url,
            }
        vm = metadata.get("vm") or {}
        if vm.get("status") != "ready":
            return {
                **lifecycle,
                "status": "unavailable",
                "code_server_url": None,
                "gitea_url": gitea_url,
            }
        if lease_id is None:
            return {
                **lifecycle,
                "status": "ready",
                "code_server_url": None,
                "gitea_url": gitea_url,
            }
        lease = await VMIdleAccessStore(dependencies.store).inspect_for_user(
            lease_id,
            owner_kind="thread",
            owner_id=thread_id,
            kind="ide",
            user_id=str(user["id"]),
        )
        if lease is None:
            return {
                **lifecycle,
                "status": "unavailable",
                "code_server_url": None,
                "code": "ide_access_expired",
                "gitea_url": gitea_url,
            }
        if dependencies.vm_ide_transport is None:
            return {
                **lifecycle,
                "status": "unavailable",
                "code_server_url": None,
                "code": "ide_guest_transport_unavailable",
                "gitea_url": gitea_url,
            }
        try:
            proof = await dependencies.vm_ide_transport.probe(
                thread_id, owner_kind="thread",
                expected_generation=lease["provision_generation"],
                expected_vm_uid=lease["vm_uid"],
            )
            if not matches_admitted_runtime(
                proof, lease["provision_generation"], lease["vm_uid"],
            ):
                raise VMIDEUnavailable("ide_runtime_changed")
        except VMIDEUnavailable as exc:
            return {
                **lifecycle,
                "status": "unavailable",
                "code_server_url": None,
                "code": exc.code,
                "gitea_url": gitea_url,
            }
        if await VMIdleAccessStore(dependencies.store).inspect_for_user(
            lease_id, owner_kind="thread", owner_id=thread_id, kind="ide",
            user_id=str(user["id"]),
        ) is None:
            return {
                **lifecycle,
                "status": "unavailable",
                "code_server_url": None,
                "code": "ide_runtime_changed",
                "gitea_url": gitea_url,
            }
        return {
            **lifecycle,
            "status": "active",
            "access_lease_id": lease_id,
            "code_server_url": _vm_thread_ide_url(thread_id, lease_id),
            "expires_at": lease["expires_at"].isoformat(),
            "gitea_url": gitea_url,
        }

    # Check VM first (takes precedence over container)
    vm_ctx = metadata.get("vm", {})
    if vm_ctx.get("status") == "ready":
        ssh_host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
        if ssh_host:
            proxy_base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
            return await contain_ide_status_for(
                thread_id,
                {
                    "status": "active",
                    "code_server_url": f"{proxy_base}/api/ide/{thread_id}/proxy/?folder=/home/agent-host/workspace",
                    "source": "live_vm",
                    "gitea_url": gitea_url,
                },
            )

    # Check workspace container (K8s pod_ip or Docker Compose ide_host)
    if ws_ctx.get("status") == "ready" and (
        ws_ctx.get("pod_ip") or ws_ctx.get("ide_host")
    ):
        proxy_base = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
        # Withhold a URL the proxy would refuse; the Gitea link survives so the
        # header keeps its one working way into the workspace.
        return await contain_ide_status_for(
            thread_id,
            {
                "status": "active",
                "code_server_url": f"{proxy_base}/api/ide/{thread_id}/proxy/?folder=/home/agent-host/workspace",
                "source": "live_workspace",
                "gitea_url": gitea_url,
            },
        )

    # Workspace is provisioning (includes "pending" from pre-provision signal)
    if ws_ctx.get("status") in ("provisioning", "pending") or vm_ctx.get("status") in (
        "provisioning",
        "pending",
    ):
        return {"status": "restoring", "code_server_url": None, "gitea_url": gitea_url}

    return {"status": "unavailable", "code_server_url": None, "gitea_url": gitea_url}


@router.post("/api/persistent/threads/{thread_id}/uploads")
async def upload_files_to_thread(
    thread_id: str,
    request: Request,
    files: list[UploadFile] = File(...),
    *,
    dependencies: ThreadFilesDependencies = Depends(get_thread_files_dependencies),
) -> dict[str, Any]:
    """Push files into a persistent thread workspace's ``uploads/`` directory.

    Lands in ``<workspace_path>/uploads/`` over SFTP for the pod/VM tiers, or
    under ``threads/<id>/uploads/`` in the object store for the ``virtual``
    tier. The cockpit then appends an ``Attached files: …`` hint to the user's
    next message so the agent can find them, identically for either transport.
    See ``services/thread_uploads.py``.

    Returns:
        ``{"thread_id": "...", "files": [{name, size, mime_type, path}, ...]}``
    """
    from orchestrator.services.thread_uploads import (
        MAX_FILES_PER_REQUEST,
        MAX_FILE_SIZE,
        MAX_TOTAL_UPLOAD_BYTES,
        ThreadUploadError,
        is_kubernetes_thread_upload_destination,
        resolve_thread_upload_destination,
        upload_files_to_attested_k8s_workspace,
        upload_files_to_attested_stateless_workspace,
        upload_files_to_attested_vm_workspace,
        upload_files_to_thread_workspace,
    )
    from shared.run_queue import LANE_STATELESS

    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )

    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    if len(files) > MAX_FILES_PER_REQUEST:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_FILES_PER_REQUEST} files per request",
        )

    async def _materialize_payloads() -> list[tuple[str, bytes, str]]:
        payloads: list[tuple[str, bytes, str]] = []
        total = 0
        for item in files:
            content = bytearray()
            legacy_read = False
            while True:
                try:
                    chunk = await item.read(1024 * 1024)
                except TypeError:
                    # Minimal UploadFile fakes and older adapters expose only
                    # read(); production Starlette uploads take a size.
                    chunk = await item.read()
                    legacy_read = True
                if not chunk:
                    break
                content.extend(chunk)
                total += len(chunk)
                if len(content) > MAX_FILE_SIZE:
                    raise ThreadUploadError(
                        413,
                        f"File '{item.filename or 'unnamed'}' exceeds "
                        f"{MAX_FILE_SIZE // (1024 * 1024)}MB",
                    )
                if total > MAX_TOTAL_UPLOAD_BYTES:
                    raise ThreadUploadError(
                        413, "Combined upload payload exceeds 300MB"
                    )
                if legacy_read:
                    break
            payloads.append(
                (
                    item.filename or "unnamed",
                    bytes(content),
                    item.content_type or "application/octet-stream",
                )
            )
        return payloads

    try:
        if thread.get("execution_lane") != LANE_STATELESS:
            fresh = await dependencies.store.get_thread(thread_id)
            if fresh is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            destination = resolve_thread_upload_destination(fresh)
            payloads = await _materialize_payloads()
            if dependencies.thread_workspace_backend(fresh) == "vm":
                lease = await _prepare_pinned_vm_thread_operation(
                    thread_id,
                    fresh,
                    destination,
                    operation_kind="thread_upload",
                    store=dependencies.store,
                    vm_provisioner=dependencies.vm_provisioner,
                    thread_workspace_backend=dependencies.thread_workspace_backend,
                )
                async with lease:

                    async def _probe_vm_runtime() -> str:
                        return (
                            "exact_live"
                            if await lease.revalidate() is not None
                            else "unknown"
                        )

                    results = await upload_files_to_attested_vm_workspace(
                        fresh,
                        payloads,
                        destination=destination,
                        expected_workspace_generation=(
                            lease.identity.workspace_generation
                        ),
                        expected_runtime_incarnation=(lease.identity.launcher_pod_uid),
                        expected_host_key_fingerprint=(
                            lease.identity.ssh_host_key_fingerprint
                        ),
                        authority_probe=_probe_vm_runtime,
                    )
            elif fresh.get(
                "execution_lane"
            ) == "pinned" and is_kubernetes_thread_upload_destination(fresh):
                (
                    generation,
                    runtime_incarnation,
                    fingerprint,
                    authority_probe,
                ) = await _prepare_pinned_k8s_thread_upload(
                    thread_id,
                    fresh,
                    destination,
                    store=dependencies.store,
                    container_provisioner=dependencies.container_provisioner,
                )
                results = await upload_files_to_attested_k8s_workspace(
                    fresh,
                    payloads,
                    destination=destination,
                    expected_workspace_generation=generation,
                    expected_runtime_incarnation=runtime_incarnation,
                    expected_host_key_fingerprint=fingerprint,
                    authority_probe=authority_probe,
                )
            else:
                results = await upload_files_to_thread_workspace(
                    fresh,
                    payloads,
                    destination=destination,
                )
        else:
            # Serialize the entire body materialization + remote write against
            # End/Resume. A marker-first upload never opens SFTP; a write-first
            # End waits until the final exact-runtime proof and byte commit.
            try:
                async with dependencies.store.stateless_session_workspace_ensure_lock(
                    thread_id,
                    wait=True,
                ) as upload_owner:
                    if not upload_owner:
                        raise HTTPException(
                            status_code=503,
                            detail="Stateless workspace lifecycle lock unavailable",
                        )
                    fresh = await dependencies.store.get_thread(thread_id)
                    if fresh is None:
                        raise HTTPException(status_code=404, detail="Thread not found")
                    metadata = thread_metadata_object(fresh)
                    try:
                        stopped = bool(stateless_stop_markers(fresh.get("metadata")))
                    except RuntimeError:
                        stopped = True
                    if (
                        fresh.get("execution_lane") != LANE_STATELESS
                        or fresh.get("status")
                        not in {"created", "active", "awaiting_user"}
                        or stopped
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail="Stateless session is not accepting uploads",
                        )
                    backend = dependencies.require_stateless_workspace(fresh)
                    destination = resolve_thread_upload_destination(fresh)
                    payloads = await _materialize_payloads()
                    if backend == "virtual":
                        results = await upload_files_to_thread_workspace(
                            fresh,
                            payloads,
                            destination=destination,
                        )
                    else:
                        workspace = metadata.get("workspace_container") or {}
                        binding = metadata.get("_workspace_binding") or {}
                        generation = str(binding.get("generation") or "")
                        runtime_incarnation = str(
                            workspace.get("_runtime_incarnation") or ""
                        )
                        fingerprint = str(binding.get("ssh_host_key_fingerprint") or "")

                        async def _probe_runtime() -> str:
                            return await dependencies.container_provisioner.workspace_pod_authority(
                                WorkspaceOwner.session(thread_id),
                                expected_runtime_incarnation=runtime_incarnation,
                            )

                        results = await upload_files_to_attested_stateless_workspace(
                            fresh,
                            payloads,
                            destination=destination,
                            expected_workspace_generation=generation,
                            expected_runtime_incarnation=runtime_incarnation,
                            expected_host_key_fingerprint=fingerprint,
                            authority_probe=_probe_runtime,
                        )
            except TimeoutError as exc:
                raise HTTPException(
                    status_code=503,
                    detail="Stateless workspace lifecycle lock timed out",
                ) from exc
    except ThreadUploadError as e:
        logger.warning(
            "Thread upload failed for %s: %d %s", thread_id, e.status_code, e.detail
        )
        raise HTTPException(status_code=e.status_code, detail=e.detail) from e

    return {
        "thread_id": thread_id,
        "files": [
            {"name": r.name, "size": r.size, "mime_type": r.mime_type, "path": r.path}
            for r in results
        ],
    }


@router.delete("/api/persistent/threads/{thread_id}/uploads/{path:path}")
async def delete_thread_upload(
    thread_id: str,
    path: str,
    request: Request,
    *,
    dependencies: ThreadFilesDependencies = Depends(get_thread_files_dependencies),
) -> dict[str, Any]:
    """Remove one file from a persistent thread workspace's ``uploads/`` dir.

    Exists so the cockpit's *eager* upload can be cancelled honestly. The
    composer starts transferring the moment a file is attached, before the
    user commits to sending, so removing an attachment chip can arrive after
    the bytes have already landed. Without this, cancelling is a lie and
    attach → remove → re-attach cycles accumulate ``_1``/``_2`` copies in a
    directory the agent can list and read
    (``knowledge-base/knowledge/features/session_attachment_send_flow.md`` §9.1).

    ``path`` is relative to ``uploads/`` — i.e. the ``name`` field the upload
    response returned (``report.pdf``, or ``bundle/sub/a.txt`` for a
    zip-extracted member), **not** its ``uploads/``-prefixed ``path`` field,
    which would resolve to ``uploads/uploads/…``. Naming a zip's ``<stem>``
    directory removes that whole subtree.

    Validation is ``_safe_upload_relpath``, which rejects rather than
    sanitizes and is never delegated to the remote: SFTP would remove any
    path the ``agent-host`` user can write, and the thread's object-store
    prefix is shared with Canvas state and tool files.

    Returns:
        ``{"thread_id": "...", "path": "uploads/<path>", "deleted": true}``,
        where ``<path>`` is the **normalized** path that was actually removed
        — ``bundle/sub/../a.txt`` in, ``uploads/bundle/a.txt`` out. Echoing
        the raw input would report a file that was never touched.
        400 for a path that escapes ``uploads/``, 404 when there is no such
        upload, and the usual destination taxonomy (409 no/unready workspace,
        502 unreachable, 503 misconfigured or at capacity) otherwise.
    """
    from orchestrator.services.thread_uploads import (
        ThreadUploadError,
        delete_file_from_attested_k8s_workspace,
        delete_file_from_attested_stateless_workspace,
        delete_file_from_attested_vm_workspace,
        delete_file_from_thread_workspace,
        is_kubernetes_thread_upload_destination,
        resolve_thread_upload_destination,
    )
    from shared.run_queue import LANE_STATELESS

    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )

    try:
        if thread.get("execution_lane") != LANE_STATELESS:
            fresh = await dependencies.store.get_thread(thread_id)
            if fresh is None:
                raise HTTPException(status_code=404, detail="Thread not found")
            destination = resolve_thread_upload_destination(fresh)
            if dependencies.thread_workspace_backend(fresh) == "vm":
                lease = await _prepare_pinned_vm_thread_operation(
                    thread_id,
                    fresh,
                    destination,
                    operation_kind="thread_delete",
                    store=dependencies.store,
                    vm_provisioner=dependencies.vm_provisioner,
                    thread_workspace_backend=dependencies.thread_workspace_backend,
                )
                async with lease:

                    async def _probe_vm_runtime() -> str:
                        return (
                            "exact_live"
                            if await lease.revalidate() is not None
                            else "unknown"
                        )

                    removed = await delete_file_from_attested_vm_workspace(
                        fresh,
                        path,
                        destination=destination,
                        expected_workspace_generation=(
                            lease.identity.workspace_generation
                        ),
                        expected_runtime_incarnation=(lease.identity.launcher_pod_uid),
                        expected_host_key_fingerprint=(
                            lease.identity.ssh_host_key_fingerprint
                        ),
                        authority_probe=_probe_vm_runtime,
                    )
            elif fresh.get(
                "execution_lane"
            ) == "pinned" and is_kubernetes_thread_upload_destination(fresh):
                (
                    generation,
                    runtime_incarnation,
                    fingerprint,
                    authority_probe,
                ) = await _prepare_pinned_k8s_thread_upload(
                    thread_id,
                    fresh,
                    destination,
                    store=dependencies.store,
                    container_provisioner=dependencies.container_provisioner,
                )
                removed = await delete_file_from_attested_k8s_workspace(
                    fresh,
                    path,
                    destination=destination,
                    expected_workspace_generation=generation,
                    expected_runtime_incarnation=runtime_incarnation,
                    expected_host_key_fingerprint=fingerprint,
                    authority_probe=authority_probe,
                )
            else:
                removed = await delete_file_from_thread_workspace(
                    fresh, path, destination=destination
                )
        else:
            try:
                async with dependencies.store.stateless_session_workspace_ensure_lock(
                    thread_id,
                    wait=True,
                ) as delete_owner:
                    if not delete_owner:
                        raise HTTPException(
                            status_code=503,
                            detail="Stateless workspace lifecycle lock unavailable",
                        )
                    fresh = await dependencies.store.get_thread(thread_id)
                    if fresh is None:
                        raise HTTPException(status_code=404, detail="Thread not found")
                    metadata = thread_metadata_object(fresh)
                    try:
                        stopped = bool(stateless_stop_markers(fresh.get("metadata")))
                    except RuntimeError:
                        stopped = True
                    if (
                        fresh.get("execution_lane") != LANE_STATELESS
                        or fresh.get("status")
                        not in {"created", "active", "awaiting_user"}
                        or stopped
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail="Stateless session is not accepting upload deletes",
                        )
                    backend = dependencies.require_stateless_workspace(fresh)
                    destination = resolve_thread_upload_destination(fresh)
                    if backend == "virtual":
                        removed = await delete_file_from_thread_workspace(
                            fresh,
                            path,
                            destination=destination,
                        )
                    else:
                        workspace = metadata.get("workspace_container") or {}
                        binding = metadata.get("_workspace_binding") or {}
                        generation = str(binding.get("generation") or "")
                        runtime_incarnation = str(
                            workspace.get("_runtime_incarnation") or ""
                        )
                        fingerprint = str(binding.get("ssh_host_key_fingerprint") or "")

                        async def _probe_runtime() -> str:
                            return await dependencies.container_provisioner.workspace_pod_authority(
                                WorkspaceOwner.session(thread_id),
                                expected_runtime_incarnation=runtime_incarnation,
                            )

                        removed = await delete_file_from_attested_stateless_workspace(
                            fresh,
                            path,
                            destination=destination,
                            expected_workspace_generation=generation,
                            expected_runtime_incarnation=runtime_incarnation,
                            expected_host_key_fingerprint=fingerprint,
                            authority_probe=_probe_runtime,
                        )
            except TimeoutError as exc:
                raise HTTPException(
                    status_code=503,
                    detail="Stateless workspace lifecycle lock timed out",
                ) from exc
    except ThreadUploadError as e:
        logger.warning(
            "Thread upload delete refused for %s (%r): %d %s",
            thread_id,
            path,
            e.status_code,
            e.detail,
        )
        raise HTTPException(status_code=e.status_code, detail=e.detail) from e

    if removed is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    return {"thread_id": thread_id, "path": f"uploads/{removed}", "deleted": True}
