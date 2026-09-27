"""The agent's thread lifecycle edges: status, suspend, release-agent.

Extracted from ``orchestrator.main`` (R1.B06, root lane). Three internal routes.
Only transport lives here — the ``X-Internal-Key`` guard, the request headers
the suspend edge reads its owner credential from, and the raw JSON body of
release-agent whose *unparseable* case is a 400. Every outcome, including the
400 for a missing ``agent_id``, is decided in
``orchestrator.services.agent_thread_status`` so it can be tested without a
request object.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from orchestrator.schemas.agent_thread_status import (
    AgentRetirementActuatorRequest,
    AgentThreadStatusRequest,
)
from orchestrator.services import agent_thread_status

# No `tags=`: the declarations this replaces carried none, and a tag would
# change the published OpenAPI operations for routes whose identity this batch
# is required to leave untouched.
router = APIRouter()


def get_agent_thread_status_dependencies(
    request: Request,
) -> agent_thread_status.AgentThreadStatusDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.agent_thread_status_dependencies_factory()


@router.post("/api/agents/threads/{thread_id}/retirement-actuator")
async def agent_request_retirement_actuator(
    request: Request, thread_id: str, body: AgentRetirementActuatorRequest,
) -> dict[str, Any]:
    dependencies = get_agent_thread_status_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_thread_status.request_retirement_actuator(
        thread_id, body, dependencies=dependencies,
    )


@router.put("/api/agents/threads/{thread_id}/status")
async def agent_update_thread_status(
    request: Request,
    thread_id: str,
    body: AgentThreadStatusRequest,
) -> dict[str, Any]:
    """Update thread status. **Internal** (P4b) — requires ``X-Internal-Key``.
    Ingress strips this path.

    Lifecycle transitions:
      created → active, active → ended (existing).
      active → awaiting_user (Phase 5: agent reached natural pause, no WS
        subscriber). Idempotent — repeated awaiting_user writes preserve
        the original awaiting_user_since so the attention-sleep watchdog's
        clock keeps ticking.
      awaiting_user → active (Phase 5: subscriber reattached). Clears
        awaiting_user_since and extend_count.

    'suspended' is reserved for the attention-sleep watchdog and is not
    writable from agent path — would create a race where an agent flips
    the thread back to active while the orchestrator is mid-suspend.
    """
    dependencies = get_agent_thread_status_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_thread_status.update_thread_status(
        thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{thread_id}/suspend")
async def agent_suspend_thread(request: Request, thread_id: str) -> dict[str, Any]:
    """Clean drain-suspend requested by the thread's own agent. **Internal**
    (P4b) — requires ``X-Internal-Key``. Ingress strips this path.

    Called by a persistent agent that received ``intents.should_drain``
    while its loop is parked between turns. An active dedicated-pod recycle
    generation derives the exact old agent/UID/drain intent under the durable
    locks and owns this request before any workspace action; this accepts the
    headerless shape sent by pre-change runtimes. Without an active recycle,
    the historical path converges on the attention-sleep state by snapshotting
    the workspace and clearing the agent binding.

    Returns ``{"suspended": bool, "status": <thread status>}``. The agent
    falls back to the legacy 'ended' detach when ``suspended`` is false —
    e.g. suspension service disabled, snapshot failure, or a thread already
    past the point of suspending.
    """
    dependencies = get_agent_thread_status_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_thread_status.suspend_thread(
        thread_id, headers=request.headers, dependencies=dependencies
    )


@router.post("/api/agents/threads/{thread_id}/release-agent")
async def agent_release_thread_agent(
    request: Request, thread_id: str
) -> dict[str, str]:
    """Clear threads.agent_id. **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    Called by an agent whose /session/attach background task failed (e.g.
    workspace SSH polling timed out before the workspace pod's image pull
    completed). Without this, the thread stays bound to a session-less agent
    and the next WS reconnect re-targets the same broken agent.
    """
    dependencies = get_agent_thread_status_dependencies(request)
    await dependencies.require_internal(request)
    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="agent_id is required") from exc
    return await agent_thread_status.release_thread_agent(
        thread_id, body, dependencies=dependencies
    )
