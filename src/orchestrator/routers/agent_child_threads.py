"""HTTP adapter for the agent-facing thread and subagent-child routes.

Extracted from ``orchestrator.main`` (R1.B06, lane C, census group
``S_CHILD``). Thirteen routes, declared in their pre-extraction order, plus
the session batch settle (``…/subagents/settle-batch``) added beside them.

**That order is load-bearing.** Starlette matches in registration order, so
``…/subagents/live``, ``…/subagents/by-call`` and ``…/subagents/settle-batch``
must stay declared *before* ``…/subagents/{thread_id}`` — flip them and
``live`` becomes a thread id.

No ``tags=``, no ``operation_id=``, no ``status_code=``, no route-level
``dependencies=``: the declarations this replaces carried none, and each would
change the published OpenAPI operation for a route this batch must leave
byte-identical.

**The internal gate is called here, as each handler's first statement**, exactly
as it was in the application module. That placement preserves the pre-extraction
ordering and is where ``scripts/check_endpoint_auth.py`` reads the audited gate
identity for ``policy/endpoint_inventory.txt``; a gate hidden one call deeper
would classify every one of these routes as ``unscoped``.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request

from orchestrator.schemas.agent_child_threads import (
    AgentSessionSubagentBatchSettleRequest,
    AgentSessionSubagentByCallRequest,
    AgentSessionSubagentCreateRequest,
    AgentSessionSubagentQueryRequest,
    AgentSessionSubagentReopenRequest,
    AgentSessionSubagentTerminalRequest,
    AgentSubagentThreadCreateRequest,
    AgentSubagentThreadQueryRequest,
    AgentSubagentThreadReopenRequest,
    AgentSubagentThreadTerminalRequest,
    AgentThreadCreateRequest,
    AgentThreadMessageRequest,
)
from orchestrator.services import agent_child_threads

router = APIRouter()


def get_agent_child_thread_dependencies(
    request: Request,
) -> agent_child_threads.AgentChildThreadDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.agent_child_threads_dependencies_factory()


@router.post("/api/agents/threads")
async def agent_create_thread(
    request: Request, body: AgentThreadCreateRequest
) -> dict[str, Any]:
    """Agent creates its own thread on startup. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Used by persistent agents starting with ORCHESTRATOR_URL set.
    Creates a thread with user_id=NULL (visible to all cockpit users).
    """
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_create_thread(
        request, body, dependencies=dependencies
    )


@router.post("/api/agents/jobs/{job_id}/subagents")
async def agent_create_subagent_thread(
    request: Request, job_id: str, body: AgentSubagentThreadCreateRequest
) -> dict[str, Any]:
    """Create the ``threads`` row of a subagent child of a job (U3 B.1).
    **Internal** — requires ``X-Internal-Key``. Ingress strips this path.

    The orchestrator owns thread creation: the row is derived from the JOB
    (``user_id`` / ``project_id`` come from ``jobs``, never from the body),
    which is what makes the child's transcript readable by the job owner
    through the ordinary thread endpoints and keeps it off every other
    user's sessions page. Nothing is provisioned — no repository, no
    workspace, no pod: a child runs inside its parent's. Compare
    ``POST /api/agents/threads``, which provisions a session.

    Idempotent per ``subagent_id`` while the parent remains open: a retried
    create returns the same id. Once a completion decision is journaled even
    an exact retry is refused, so completion cannot race a child revival.
    404 when the job does not exist (the FK would refuse the row anyway).
    """
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_create_subagent_thread(
        request, job_id, body, dependencies=dependencies
    )


@router.post("/api/agents/jobs/{job_id}/subagents/live")
async def agent_list_live_subagent_threads(
    request: Request, job_id: str, body: AgentSubagentThreadQueryRequest
) -> dict[str, Any]:
    """List generation-bearing queued/running children. Internal only."""
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_list_live_subagent_threads(
        request, job_id, body, dependencies=dependencies
    )


@router.post("/api/agents/jobs/{job_id}/subagents/{thread_id}")
async def agent_get_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadQueryRequest,
) -> dict[str, Any]:
    """Read one exact worker child, including its generation. Internal only."""
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_get_subagent_thread(
        request, job_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/jobs/{job_id}/subagents/{thread_id}/reopen")
async def agent_reopen_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadReopenRequest,
) -> dict[str, Any]:
    """Rotate an ended child to a queued generation. Internal only."""
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_reopen_subagent_thread(
        request, job_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/jobs/{job_id}/subagents/{thread_id}/terminal")
async def agent_terminalize_subagent_thread(
    request: Request,
    job_id: str,
    thread_id: UUID,
    body: AgentSubagentThreadTerminalRequest,
) -> dict[str, Any]:
    """Atomically terminalize one run and enqueue its stable report."""
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_terminalize_subagent_thread(
        request, job_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents")
async def agent_create_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentCreateRequest,
) -> dict[str, Any]:
    """Create a child of one exact persistent-session runtime. Internal only."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_create_session_subagent_thread(
        request, parent_thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/live")
async def agent_list_live_session_subagent_threads(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentQueryRequest,
) -> dict[str, Any]:
    """List session child recovery candidates under exact authority."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_list_live_session_subagent_threads(
        request, parent_thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/by-call")
async def agent_get_session_subagent_thread_by_call(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentByCallRequest,
) -> dict[str, Any]:
    """Resolve a replayed session delegation call. Internal only."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_get_session_subagent_thread_by_call(
        request, parent_thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/settle-batch")
async def agent_settle_session_subagent_batch(
    request: Request,
    parent_thread_id: str,
    body: AgentSessionSubagentBatchSettleRequest,
) -> dict[str, Any]:
    """Settle one abandoned session delegation turn atomically. Internal only."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_settle_session_subagent_batch(
        request, parent_thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/{thread_id}")
async def agent_get_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentQueryRequest,
) -> dict[str, Any]:
    """Read one session child and its generation. Internal only."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_get_session_subagent_thread(
        request, parent_thread_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/{thread_id}/reopen")
async def agent_reopen_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentReopenRequest,
) -> dict[str, Any]:
    """Rotate one terminal session child generation. Internal only."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_reopen_session_subagent_thread(
        request, parent_thread_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{parent_thread_id}/subagents/{thread_id}/terminal")
async def agent_terminalize_session_subagent_thread(
    request: Request,
    parent_thread_id: str,
    thread_id: UUID,
    body: AgentSessionSubagentTerminalRequest,
) -> dict[str, Any]:
    """End a session child and atomically persist a background report event."""

    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_terminalize_session_subagent_thread(
        request, parent_thread_id, thread_id, body, dependencies=dependencies
    )


@router.post("/api/agents/threads/{thread_id}/messages")
async def agent_save_message(
    request: Request,
    thread_id: str,
    body: AgentThreadMessageRequest,
) -> dict[str, Any]:
    """Agent saves a message to thread history. **Internal** (P4b) —
    requires ``X-Internal-Key``. Ingress strips this path.

    Fire-and-forget safe — agents call this after each turn.
    """
    dependencies = get_agent_child_thread_dependencies(request)
    await dependencies.require_internal(request)
    return await agent_child_threads.agent_save_message(
        request, thread_id, body, dependencies=dependencies
    )


__all__ = [
    "agent_create_session_subagent_thread",
    "agent_create_subagent_thread",
    "agent_create_thread",
    "agent_get_session_subagent_thread",
    "agent_get_session_subagent_thread_by_call",
    "agent_get_subagent_thread",
    "agent_list_live_session_subagent_threads",
    "agent_list_live_subagent_threads",
    "agent_reopen_session_subagent_thread",
    "agent_reopen_subagent_thread",
    "agent_save_message",
    "agent_settle_session_subagent_batch",
    "agent_terminalize_session_subagent_thread",
    "agent_terminalize_subagent_thread",
    "get_agent_child_thread_dependencies",
    "router",
]
