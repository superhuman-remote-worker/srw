"""HTTP adapter for agent registration, heartbeat and the fleet admin reads.

Extracted from ``orchestrator.main`` (R1.B06, lane C, census group ``S_REG``).
Seven routes, in their pre-extraction declaration order.

Nothing but the path, method, handler name, return annotation and — for
``/api/agents/register`` — the explicit ``response_model`` is declared here.
No ``tags=``, no ``operation_id=``, no ``status_code=``, no route-level
``dependencies=``: the declarations this replaces carried none, and each would
change the published OpenAPI operation for a route this batch must leave
byte-identical. ``list_agents`` keeps its ``Query`` defaults verbatim, bounds
included, because those bounds are the published request contract.

**The fleet admin routes** (list, read, system info, deregister) are served
under ``/api/admin/agents``. ``/api/agents`` is the agent-internal prefix, and
an edge that blocks it (the dev tunnel does) left the Cockpit's Agents panel
empty. Each still answers on its old ``/api/agents`` path through a second,
``deprecated=True`` decorator — the same handler, gate and response — for one
release, so an already-loaded Cockpit and other old clients keep working.
**Remove those decorators in a later release** — except ``DELETE
/api/agents/{agent_id}``, which is also the agent's own internal-key
deregistration (``OrchestratorClient.deregister``) and stays; only its admin
use is deprecated there.

**The access gate is called here, as the handler's first statement**, exactly as
it was in the application module. That placement is load-bearing twice over: it
preserves the pre-extraction ordering (nothing is read or written before the
refusal), and it is where ``scripts/check_endpoint_auth.py`` reads the audited
gate identity for ``policy/endpoint_inventory.txt``. A gate hidden one call
deeper would classify these routes as ``unscoped``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request

from orchestrator.schemas.agent_registration import PodRuntimeActorRequest
from orchestrator.schemas.agent_runtime import (
    AgentHeartbeat,
    AgentRegistration,
    AgentRegistrationResponse,
)
from orchestrator.services import agent_registration

router = APIRouter()


def get_agent_registration_dependencies(
    request: Request,
) -> agent_registration.AgentRegistrationDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.agent_registration_dependencies_factory()


@router.post("/api/agents/register", response_model=AgentRegistrationResponse)
async def register_agent(
    request: Request, registration: AgentRegistration
) -> AgentRegistrationResponse:
    """Register a new agent or update existing one. **Internal** (P4b) —
    requires ``X-Internal-Key``. Public ingress also strips this path.

    When an agent starts up, it calls this endpoint to register itself.
    If an agent with the same hostname exists, its pod_ip is updated.

    Returns:
        AgentRegistrationResponse with agent_id and heartbeat_interval_seconds
    """
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_registration.register_agent(
        request, registration, dependencies=dependencies
    )


@router.post("/api/agents/{agent_id}/runtime-actor/session")
async def agent_pod_runtime_actor(
    request: Request, agent_id: str, body: PodRuntimeActorRequest
) -> dict[str, Any]:
    """Bind a warm pool pod's thread-less bootstrap to the session it just got.
    **Internal** (P4b) — requires ``X-Internal-Key`` *and* the pod bootstrap
    header. Ingress strips this path.

    A dedicated session pod does this inside ``/api/agents/register``, where
    the thread is known at provision time. A pool pod cannot: it registers
    thread-less and is handed a session later over ``/session/attach``, and K8s
    env is not patchable on a running pod. Without this route the pod runs the
    session with no actor identity at all and every sensitive knowledge write
    fails ``missing_credential`` — the failure BP-05's live gate hit.

    The shared internal key is deliberately insufficient here, exactly as it is
    at registration: the caller must also present the unique bootstrap injected
    into its own pod, and the thread binding is read from the ``agents`` row
    rather than believed from the body.
    """
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_registration.agent_pod_runtime_actor(
        request, agent_id, body, dependencies=dependencies
    )


@router.post("/api/agents/{agent_id}/heartbeat")
async def agent_heartbeat(
    request: Request, agent_id: str, heartbeat: AgentHeartbeat
) -> dict[str, Any]:
    """Update agent heartbeat and status. **Internal** (P4b) — requires
    ``X-Internal-Key``. Ingress strips this path.

    Agents call this every 60 seconds to report their status.
    The orchestrator uses this to track agent health and current job state.
    """
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_registration.agent_heartbeat(
        request, agent_id, heartbeat, dependencies=dependencies
    )


@router.get("/api/agents", deprecated=True)
@router.get("/api/admin/agents")
async def list_agents(
    request: Request,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """List all registered agents. **Admin only** (G4) — exposes pod IPs,
    hostnames, and full fleet metadata. Non-admins must use
    `/api/me/active-jobs` for a stripped, per-user projection of their
    in-flight work.

    Args:
        status: Optional status filter (booting, ready, working, completed, failed, offline)
        limit: Maximum agents to return
    """
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_admin(request)
    return await agent_registration.list_agents(
        request, status, limit, dependencies=dependencies
    )


@router.get("/api/agents/{agent_id}", deprecated=True)
@router.get("/api/admin/agents/{agent_id}")
async def get_agent(request: Request, agent_id: str) -> dict[str, Any]:
    """Get agent details by ID. **Admin only** (G4)."""
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_admin(request)
    return await agent_registration.get_agent(
        request, agent_id, dependencies=dependencies
    )


@router.get("/api/agents/{agent_id}/system-info", deprecated=True)
@router.get("/api/admin/agents/{agent_id}/system-info")
async def get_agent_system_info(request: Request, agent_id: str) -> dict[str, Any]:
    """Proxy system info request to an agent's /system/info endpoint.
    **Admin only** (G4) — proxies host-level CPU/memory/process/port
    inventory from the agent container.

    Returns CPU, memory, disk, processes, listening ports, and network
    connections from the agent's container.
    """
    dependencies = get_agent_registration_dependencies(request)
    await dependencies.require_admin(request)
    return await agent_registration.get_agent_system_info(
        request, agent_id, dependencies=dependencies
    )


# The old path is not deprecated as a whole: agents deregister on it. Only an
# admin calling it is (see the module docstring).
@router.delete("/api/agents/{agent_id}")
@router.delete("/api/admin/agents/{agent_id}")
async def delete_agent(request: Request, agent_id: str) -> dict[str, str]:
    """Deregister an agent. **Admin or internal-key** (G4).

    Used by the cockpit's agent-list admin tool (``/api/admin/agents/{id}``;
    admin use of ``/api/agents/{id}`` is deprecated), and by agents
    deregistering on graceful shutdown via X-Internal-Key on
    ``/api/agents/{id}`` so clean exits stop aging into missed-heartbeat
    corpses (Track B will move them to a bearer-credentialled path). The
    heartbeat timeout (3min) remains the backstop for crashes.

    Only a bare internal-key call skips the admin gate. The MCP server sends
    the key with ``X-MCP-User-Id`` on every tool call a user drives, so the
    MCP ``deregister_agent`` tool is an admin call like the cockpit's (N9).
    """
    dependencies = get_agent_registration_dependencies(request)
    if not dependencies.is_internal_service_call(request):
        await dependencies.require_admin(request)
    return await agent_registration.delete_agent(
        request, agent_id, dependencies=dependencies
    )


__all__ = [
    "agent_heartbeat",
    "agent_pod_runtime_actor",
    "delete_agent",
    "get_agent",
    "get_agent_registration_dependencies",
    "get_agent_system_info",
    "list_agents",
    "register_agent",
    "router",
]
