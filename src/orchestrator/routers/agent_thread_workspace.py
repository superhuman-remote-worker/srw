"""The internal agent's workspace poll for a thread.

Extracted from ``orchestrator.main`` (R1.B05, root lane). One route, and the
lock it holds is the point: this response is a credential-delivery boundary for
cold and dedicated sessions, so the same lock that guards a live selection
replacement is taken *around* every authoritative read and the whole response
build. Once an ``A -> B/[]`` save commits, no later cold response can still
deliver the old ``A`` payload.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from orchestrator.services import cloud_mount_status, thread_workspace_delivery
from orchestrator.services.cloud_mount_sidecar import (
    DELIVERY_HEADER,
    recorded_sidecar_plan,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

# No `tags=`: the declaration this replaces carried none, and a tag would
# change the published OpenAPI operation for a route whose identity this
# batch is required to leave untouched.
router = APIRouter()


def get_thread_workspace_dependencies(
    request: Request,
) -> thread_workspace_delivery.ThreadWorkspaceDeliveryDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.thread_workspace_delivery_dependencies_factory()


@router.get("/api/agents/threads/{thread_id}/workspace")
async def agent_get_thread_workspace(
    request: Request, thread_id: str
) -> dict[str, Any]:
    """Agent polls workspace container status for a thread. **Internal**
    (P4b) — requires ``X-Internal-Key``. Ingress strips this path.

    Returns workspace_container metadata from the thread,
    allowing the agent to wait for the workspace to be ready.
    """
    dependencies = get_thread_workspace_dependencies(request)
    await dependencies.require_internal(request)
    request_headers = getattr(request, "headers", {})
    presented_agent_id = request_headers.get("X-Agent-ID")
    presented_runtime_generation = request_headers.get("X-Session-Runtime-Generation")
    presented_attach_token = request_headers.get("X-Session-Runtime-Attach-Token")
    # This response is a credential-delivery boundary for cold/dedicated
    # sessions.  Use the same lock as live selection replacement and do every
    # authoritative read + response build beneath it.  Once an A -> B/[] save
    # commits, no later cold response can deliver the old A payload. The
    # lease delivery's network part runs first, outside the lock.
    await thread_workspace_delivery.prepare_agent_thread_workspace(
        thread_id, dependencies=dependencies
    )
    async with dependencies.store.thread_datasource_lock(thread_id):
        payload = await thread_workspace_delivery.agent_get_thread_workspace_locked(
            thread_id,
            presented_agent_id=presented_agent_id,
            presented_runtime_generation=presented_runtime_generation,
            presented_attach_token=presented_attach_token,
            presented_cloud_mount_delivery=request_headers.get(DELIVERY_HEADER),
            dependencies=dependencies,
        )
    if payload.get("cloud_mount_agent_outdated"):
        # An agent image from before the in-pod plane attached to a Pod whose
        # mounts come from its sidecars: say so where the user looks.
        await cloud_mount_status.record_agent_outdated(
            dependencies.store, thread_id, payload.get("cloud_mount_sidecar") or {}
        )
    return payload


@router.post("/api/agents/threads/{thread_id}/cloud-mount-status")
async def agent_report_cloud_mount_status(
    request: Request, thread_id: str
) -> dict[str, Any]:
    """The agent reports the state of the cloud folders its workspace Pod's
    sidecars mounted (connector drivers D7). **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    Kept only for the plan the thread's current Pod records (the report names
    its fingerprint), checked against that plan and the closed state and
    reason sets; a report about another Pod's plan is a 409. Nothing here is
    a credential or a remote's own words.
    """
    dependencies = get_thread_workspace_dependencies(request)
    await dependencies.require_internal(request)
    try:
        body = await request.json()
    except ValueError:
        raise HTTPException(status_code=422, detail="body must be JSON") from None
    if not isinstance(body, dict) or not isinstance(body.get("fingerprint"), str):
        raise HTTPException(status_code=422, detail="fingerprint is required")
    thread = await dependencies.store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="thread not found")
    recorded = recorded_sidecar_plan(thread_metadata_object(thread))
    if recorded is None or recorded.get("fingerprint") != body["fingerprint"]:
        raise HTTPException(
            status_code=409, detail="the report is not about this thread's Pod"
        )
    try:
        entries = cloud_mount_status.report_entries(recorded, body.get("mounts"))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    recorded_ok = await cloud_mount_status.record_report(
        dependencies.store,
        thread_id,
        fingerprint=body["fingerprint"],
        entries=entries,
    )
    if not recorded_ok:
        raise HTTPException(
            status_code=409, detail="the report is not about this thread's Pod"
        )
    return {"ok": True}


__all__ = [
    "agent_get_thread_workspace",
    "agent_report_cloud_mount_status",
    "get_thread_workspace_dependencies",
    "router",
]
