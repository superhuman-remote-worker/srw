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

from fastapi import APIRouter, Request

from orchestrator.services import connector_bind_time, thread_workspace_delivery

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
    # A registered driver's connector binds in its own pod (D6): waited for
    # here, outside the lock and under the agent's request timeout; one
    # still binding is delivered later with a README notice meanwhile.
    await connector_bind_time.prepare_thread_bindings(dependencies.store, thread_id)
    # This response is a credential-delivery boundary for cold/dedicated
    # sessions.  Use the same lock as live selection replacement and do every
    # authoritative read + response build beneath it.  Once an A -> B/[] save
    # commits, no later cold response can deliver the old A payload. The
    # lease delivery's network part runs first, outside the lock.
    await thread_workspace_delivery.prepare_agent_thread_workspace(
        thread_id, dependencies=dependencies
    )
    async with dependencies.store.thread_datasource_lock(thread_id):
        return await thread_workspace_delivery.agent_get_thread_workspace_locked(
            thread_id,
            presented_agent_id=presented_agent_id,
            presented_runtime_generation=presented_runtime_generation,
            presented_attach_token=presented_attach_token,
            dependencies=dependencies,
        )


__all__ = ["agent_get_thread_workspace", "get_thread_workspace_dependencies", "router"]
