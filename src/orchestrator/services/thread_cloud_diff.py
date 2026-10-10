"""Protected cloud mode (Slice C, Task 8): owner-facing cloud-diff review.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane M).

Five operations share one gate (``_require_protected``) and one resolver
(``_thread_cloud_diff_source``) that builds a Task 7 ``UpperdirDiffSource``
from the thread's ``cloud_ro_mounts`` row + selected ``thread_mounts`` row.
See knowledge-base/knowledge/design/cloud_access_unification.md §5/§11 and
.superpowers/sdd/task-8-brief.md for the response-shape contract Cockpit
(Task 14) depends on.

Three properties are load-bearing and moved unchanged:

* **Reads survive the runtime.** ``_thread_cloud_diff_source`` deliberately
  does not require an *active* grant, so an ended thread whose reader was
  already reconciled away stays reviewable. Only restage/apply-side workspace
  steps need a live pod.
* **Overlay reset is bound to the producer of the reviewed bytes.**
  ``_capture_thread_overlay_reset_authority`` refuses unless the current
  runtime is byte-for-byte the one that staged the summary, so a resumed
  successor, a rebound pool agent or a replaced workspace is never contacted
  with an unauthenticated reset. Old summaries without producer identity fail
  closed.
* **404 over 403.** A non-protected thread is indistinguishable from a
  missing one through this surface.

Collaborators arrive through :class:`ThreadCloudDiffDependencies`, rebuilt per
invocation by the application.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import httpx
from fastapi import HTTPException

from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_stage_authority import (
    _broadcast_cloud_stage_result,
    _capture_cloud_stage_authority,
    _cloud_stage_task_key,
    _thread_selected_vm_workspace,
)
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.protected_cloud_engage import (
    ProtectedCloudEngageDependencies,
    _resolve_protected_reader_backend,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

logger = logging.getLogger(__name__)

_EMPTY_CLOUD_DIFF_COUNTS = {"added": 0, "modified": 0, "deleted": 0}


@dataclass(frozen=True)
class ThreadCloudDiffDependencies:
    """Collaborators for one cloud-diff operation, resolved per invocation.

    ``store`` is main's ``postgres_db``, ``cloud_router`` its
    ``main_cloud_router`` (passed straight through to ``apply_staged_diff``),
    ``snapshot_service`` and ``vm_provisioner`` the process singletons main
    imports, and ``cloud_tasks`` the application-owned ``CloudTaskRegistry``
    (``stage_has`` / ``stage_start``) that replaces main's
    ``_cloud_stage_tasks`` dict. ``protected_cloud`` is the sibling
    dependencies object ``_resolve_protected_reader_backend`` needs.
    """

    store: Any
    cloud_router: Any
    snapshot_service: Any
    vm_provisioner: Any
    cloud_tasks: Any
    protected_cloud: ProtectedCloudEngageDependencies
    is_protected_cloud_mode_enabled: Callable[[], bool]


def _require_protected(
    thread: dict[str, Any], *, dependencies: ThreadCloudDiffDependencies
) -> dict[str, Any]:
    """404s unless ``thread`` is protected-cloud AND the flag is on.

    Returns the parsed metadata dict (asyncpg can hand JSONB back as a raw
    string — see the isinstance guard repeated throughout this module) so
    callers that need it further (restage's workspace-host check) don't have
    to re-parse.
    """
    metadata = thread.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}
    if (
        not metadata.get("protected_cloud")
        or not dependencies.is_protected_cloud_mode_enabled()
    ):
        raise HTTPException(
            status_code=404, detail="Thread is not in protected cloud mode."
        )
    return metadata


async def _thread_cloud_diff_source(
    thread_id: str,
    thread: dict[str, Any],
    *,
    dependencies: ThreadCloudDiffDependencies,
):
    """(mount_row, UpperdirDiffSource|None, protected_mount_name|None) for a
    protected thread — shared by the summary and per-file endpoints below.

    Deliberately does NOT require ``row["status"] == "active"``: revoked-but-
    staged rows (ended threads, grant already reconciled away by the
    reconciler) stay reviewable — spec §11. Only restage/apply-side workspace
    steps need a live pod.
    """
    from orchestrator.services.diff_source import UpperdirDiffSource

    row = await dependencies.store.get_ro_mount_by_thread(thread_id)
    if not row:
        return None, None, None
    plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(row)
    if plan is None:
        # A legacy summary has no immutable source. It stays rejectable through
        # the reject endpoint, but review/apply must never relabel it from the
        # thread's mutable current mount selection.
        return row, None, None
    backend = await _resolve_protected_reader_backend(
        plan, dependencies=dependencies.protected_cloud
    )
    handle = plan.to_project_folder_handle()
    src = UpperdirDiffSource(
        thread_id=thread_id,
        mount_row=row,
        backend=backend,
        handle=handle,
        snapshot_service=dependencies.snapshot_service,
    )
    name = plan.source.target_path
    return row, src, name


async def get_thread_cloud_diff_summary(
    *,
    thread_id: str,
    thread: dict[str, Any],
    dependencies: ThreadCloudDiffDependencies,
) -> dict[str, Any]:
    """Protected cloud mode diff summary — owner-facing review surface (Task 8).

    Reads work for ENDED threads too (mount row revoked, ``staged_summary``
    still present) — spec §11; only restage below needs a live workspace.
    Returns ``epoch=0``/empty ``files``/all-zero ``counts`` when nothing has
    been staged yet (no mount row, or a mount row with no staged_summary).
    """
    _require_protected(thread, dependencies=dependencies)

    _, src, protected_mount = await _thread_cloud_diff_source(
        thread_id, thread, dependencies=dependencies
    )
    summary = await src.summary() if src is not None else None
    if summary is None:
        return {
            "thread_id": thread_id,
            "epoch": 0,
            "staged_at": None,
            "counts": dict(_EMPTY_CLOUD_DIFF_COUNTS),
            "protected_mount": protected_mount,
            "files": [],
        }
    return {
        "thread_id": thread_id,
        "epoch": summary.meta.get("epoch") or 0,
        "staged_at": summary.meta.get("staged_at"),
        "counts": summary.meta.get("counts") or dict(_EMPTY_CLOUD_DIFF_COUNTS),
        "protected_mount": protected_mount,
        "files": [
            {"path": f.path, "status": f.status, "binary": f.binary}
            for f in summary.files
        ],
    }


async def get_thread_cloud_diff_file(
    *,
    thread_id: str,
    file_path: str,
    thread: dict[str, Any],
    dependencies: ThreadCloudDiffDependencies,
) -> dict[str, Any]:
    """Protected cloud mode per-file diff content (Task 8).

    404 when the path isn't in the staged diff, including "nothing staged at
    all" and "staged but unreadable" — ``UpperdirDiffSource.file()`` returns
    ``None`` for all three.

    The 404 body carries a ``code`` distinguishing the last case from the
    first two, because the review UI has to explain what happened and the
    three explanations are different: the path left the staged set (the
    session re-staged, or the diff was resolved elsewhere) versus the staged
    tar being missing or failing its content-binding check. Cockpit told every
    reviewer "the session has re-staged", which is wrong for a torn
    manifest/tar pair. The summary is memoized on the source, so the extra
    lookup below costs no I/O.
    """
    _require_protected(thread, dependencies=dependencies)

    _, src, _ = await _thread_cloud_diff_source(
        thread_id, thread, dependencies=dependencies
    )
    content = await src.file(file_path) if src is not None else None
    if content is None:
        summary = await src.summary() if src is not None else None
        listed = summary is not None and any(f.path == file_path for f in summary.files)
        raise HTTPException(
            status_code=404,
            detail=(
                {
                    "code": "staged_content_unreadable",
                    "message": (
                        f"Path '{file_path}' is staged but its content could "
                        "not be read."
                    ),
                }
                if listed
                else {
                    "code": "not_in_staged_diff",
                    "message": f"Path '{file_path}' is not in the staged diff.",
                }
            ),
        )
    return {
        "thread_id": thread_id,
        "path": content.path,
        "status": content.status,
        "old_content": content.old_content,
        "new_content": content.new_content,
        "old_binary": content.old_binary,
        "new_binary": content.new_binary,
    }


async def restage_thread_cloud_diff(
    *,
    thread_id: str,
    thread: dict[str, Any],
    dependencies: ThreadCloudDiffDependencies,
) -> dict[str, Any]:
    """Owner-triggered refresh of the staged protected-cloud diff (Task 8).

    Schedules the same ``stage_thread_cloud_diff`` background task the
    turn-end internal ping uses (the application's cloud-stage task registry,
    Task 5), fire-and-forget. Unlike the read endpoints above, restage needs a
    LIVE workspace: 409 ``{"code": "no_workspace"}`` when the thread's
    workspace host can't be resolved (ended thread, pod not yet ready, etc).
    """
    metadata = _require_protected(thread, dependencies=dependencies)

    from orchestrator.services.cloud_staging.stage import (
        _resolve_workspace_ssh,
        stage_thread_cloud_diff,
    )

    postgres_db = dependencies.store
    snapshot_service = dependencies.snapshot_service

    if _resolve_workspace_ssh(metadata) is None:
        raise HTTPException(status_code=409, detail={"code": "no_workspace"})
    row = await postgres_db.get_ro_mount_by_thread(thread_id)
    authority = _capture_cloud_stage_authority(thread, row or {})
    if (authority is None and not _thread_selected_vm_workspace(thread)) or (
        authority is not None and authority.get("runtime_retirement_token") is not None
    ):
        raise HTTPException(
            status_code=409, detail={"code": "cloud_stage_authority_unavailable"}
        )
    task_key = _cloud_stage_task_key(thread_id, authority)

    async def _run() -> None:
        async with postgres_db.thread_advisory_lock(thread_id):
            result = await stage_thread_cloud_diff(
                thread_id=thread_id,
                postgres_db=postgres_db,
                snapshot_service=snapshot_service,
                authority=authority,
                vm_provisioner=dependencies.vm_provisioner,
            )
        _broadcast_cloud_stage_result(result)

    # The registry is the de-duping create: a no-op when the key is already
    # present, and its own done-callback pops the slot. Main's ``try/finally``
    # around the body is therefore owned by the registry, not by this task.
    dependencies.cloud_tasks.stage_start(task_key, _run)
    return {"scheduled": True}


def _current_thread_overlay_reset_authority(
    thread: Mapping[str, Any],
) -> dict[str, str] | None:
    """Return one exact live protected runtime/workspace identity."""

    metadata = thread_metadata_object(thread)
    workspace = metadata.get("workspace_container") or {}
    binding = metadata.get("_workspace_binding") or {}
    authority = {
        "agent_id": str(thread.get("agent_id") or ""),
        "runtime_generation": str(thread.get("runtime_generation") or ""),
        "runtime_attach_token": str(thread.get("runtime_attach_token") or ""),
        "workspace_generation": str(binding.get("generation") or ""),
        "workspace_runtime_incarnation": str(
            workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or ""
        ),
    }
    try:
        for value in authority.values():
            UUID(value)
    except (TypeError, ValueError):
        return None
    if not (
        thread.get("execution_lane") == "pinned"
        and metadata.get("protected_cloud") is True
        and workspace.get("status") == "ready"
        and binding.get("kind") == "remote"
        and str(workspace.get("_canvas_workspace_generation") or "")
        == authority["workspace_generation"]
    ):
        return None
    return authority


def _capture_thread_overlay_reset_authority(
    thread: Mapping[str, Any], staged_summary: Mapping[str, Any] | None
) -> dict[str, str] | None:
    """Bind overlay reset to the producer of the reviewed staged bytes.

    A review may survive End/Resume.  In that case the current G2 runtime is
    intentionally *not* the G1 producer and must retain its unreviewed
    upperdir.  Old summaries without producer identity fail closed.
    """

    current = _current_thread_overlay_reset_authority(thread)
    producer = (
        staged_summary.get("producer") if isinstance(staged_summary, Mapping) else None
    )
    if current is None or not isinstance(producer, Mapping):
        return None
    if any(str(producer.get(key) or "") != value for key, value in current.items()):
        return None
    return current


async def _reset_thread_overlay(
    thread_id: str,
    authority: Mapping[str, str] | None,
    *,
    dependencies: ThreadCloudDiffDependencies,
) -> bool:
    """Reset only the exact protected runtime whose bytes were reviewed.

    This remains best-effort: an ended/dead runtime legitimately returns
    ``False``.  Unlike the old name-only proxy, however, a resumed successor,
    rebound pool agent, or replaced workspace is never contacted with an
    unauthenticated reset.
    """

    if authority is None:
        return False
    postgres_db = dependencies.store
    expected = dict(authority)
    agent_id = expected.get("agent_id")
    generation = expected.get("runtime_generation")
    attach_token = expected.get("runtime_attach_token")
    if not agent_id or not generation or not attach_token:
        return False
    try:
        current = await postgres_db.get_thread(thread_id)
        if (
            current is None
            or current.get("runtime_retirement_token") is not None
            or _current_thread_overlay_reset_authority(current) != expected
        ):
            return False
        agent = await postgres_db.get_agent(str(agent_id))
        if not (
            agent
            and agent.get("pod_ip")
            and str(agent.get("id") or agent_id) == str(agent_id)
            and str(agent.get("thread_id") or thread_id) == str(thread_id)
        ):
            return False
        # This is deliberately the final database authority boundary.  A
        # concurrent process restart after it is still rejected by the agent,
        # which validates the same generation/attach/workspace tuple locally
        # before touching the overlay.
        current = await postgres_db.get_thread(thread_id)
        if (
            current is None
            or current.get("runtime_retirement_token") is not None
            or _current_thread_overlay_reset_authority(current) != expected
            or not await postgres_db.pinned_thread_agent_is_reciprocal(
                thread_id,
                str(agent_id),
                expected_runtime_generation=str(generation),
                expected_attach_token=str(attach_token),
            )
        ):
            return False
        url = f"http://{agent['pod_ip']}:{agent['pod_port']}/cloud-overlay/reset"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                url,
                headers={
                    "X-Agent-ID": str(agent_id),
                    "X-Session-Runtime-Generation": str(generation),
                    "X-Session-Runtime-Attach-Token": str(attach_token),
                },
                json={
                    "thread_id": str(thread_id),
                    "workspace_generation": expected["workspace_generation"],
                    "workspace_runtime_incarnation": expected[
                        "workspace_runtime_incarnation"
                    ],
                },
            )
        return response.status_code == 200
    except Exception as e:
        logger.warning(
            "cloud-overlay reset failed for thread %s (agent %s): %s",
            thread_id,
            agent_id,
            e,
        )
        return False


async def apply_thread_cloud_diff(
    *,
    thread_id: str,
    thread: dict[str, Any],
    body: dict,
    dependencies: ThreadCloudDiffDependencies,
    user: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Owner-triggered apply of the staged protected-cloud diff (Task 10).

    Whole-diff, epoch-pinned write-back to the real cloud folder — see
    ``services.cloud_staging.apply`` module docstring for the full flow and
    its invariants (conflict gate, deletes-first, fail-soft partial writes,
    baseline re-capture on full success). The caller (``user``) must be an
    owner or editor of the protected folder's project now, whichever project
    the session belongs to (main cloud slice 5): ``403
    protected_cloud_apply_not_member`` otherwise.
    """
    _require_protected(thread, dependencies=dependencies)
    from orchestrator.services.cloud_staging.apply import (
        StagedApplyError,
        apply_staged_diff,
    )
    from orchestrator.services.protected_cloud_engage import (
        protected_source_admissible,
    )

    postgres_db = dependencies.store

    try:
        epoch = int(body.get("epoch", -1))
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail={"code": "invalid_epoch"})
    reset_row = await postgres_db.get_ro_mount_by_thread(thread_id)
    if user is not None and reset_row is not None:
        binding = reset_row.get("source_binding")
        if not await protected_source_admissible(
            str(user.get("id") or ""),
            binding if isinstance(binding, dict) else None,
            store=postgres_db,
        ):
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "protected_cloud_apply_not_member",
                    "message": (
                        "Only an owner or editor of the protected folder's "
                        "project can apply its changes"
                    ),
                },
            )
    reset_summary = (
        reset_row.get("staged_summary")
        if reset_row and int(reset_row.get("staged_epoch") or -1) == epoch
        else None
    )
    reset_authority = _capture_thread_overlay_reset_authority(thread, reset_summary)
    try:
        result = await apply_staged_diff(
            thread_id=thread_id,
            epoch=epoch,
            postgres_db=postgres_db,
            main_cloud_router=dependencies.cloud_router,
            snapshot_service=dependencies.snapshot_service,
            reset_agent_overlay=lambda: _reset_thread_overlay(
                thread_id, reset_authority, dependencies=dependencies
            ),
        )
    except StagedApplyError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    if result.get("errors"):
        raise HTTPException(
            status_code=502, detail={"code": "partial_write_failure", **result}
        )
    return {"thread_id": thread_id, **result}


async def reject_thread_cloud_diff(
    *,
    thread_id: str,
    thread: dict[str, Any],
    body: dict,
    dependencies: ThreadCloudDiffDependencies,
) -> dict[str, Any]:
    """Owner-triggered rejection of the staged protected-cloud diff (Task 10).

    Same epoch pin as apply, but never touches the cloud — see
    ``services.cloud_staging.apply`` module docstring.
    """
    _require_protected(thread, dependencies=dependencies)
    from orchestrator.services.cloud_staging.apply import (
        StagedApplyError,
        reject_staged_diff,
    )

    postgres_db = dependencies.store

    try:
        epoch = int(body.get("epoch", -1))
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail={"code": "invalid_epoch"})
    reset_row = await postgres_db.get_ro_mount_by_thread(thread_id)
    reset_summary = (
        reset_row.get("staged_summary")
        if reset_row and int(reset_row.get("staged_epoch") or -1) == epoch
        else None
    )
    reset_authority = _capture_thread_overlay_reset_authority(thread, reset_summary)
    try:
        result = await reject_staged_diff(
            thread_id=thread_id,
            epoch=epoch,
            postgres_db=postgres_db,
            snapshot_service=dependencies.snapshot_service,
            reset_agent_overlay=lambda: _reset_thread_overlay(
                thread_id, reset_authority, dependencies=dependencies
            ),
        )
    except StagedApplyError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)
    return {"thread_id": thread_id, **result}
