"""The agent's question about a git swap binding's driver pod (C3).

A workspace's first clone through the git swap driver waits for the
binding's pod, for up to the binding's ``wait_seconds``. When the reconciler
refuses that pod (an upstream that does not resolve, an egress it may not
open), the agent asks here between its tries
(``shared.connectors.git_swap.DRIVER_STATE_PATH``) and stops waiting with
the refusal's fixed reason instead of running out the wait.

**Internal** (``X-Internal-Key``, as every agent route; the ingress strips
``/api/agents``). The binding's own lease token, in the body, is the
authority for the answer: an execution learns about the bindings it holds a
lease for and nothing else, and the answer is a fixed reason, never the
reconciler's or the upstream's words. Nothing here logs the token.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.services.connector_git_swap_delivery import binding_driver_state
from orchestrator.services.connector_lease_exchange import NO_STORE

router = APIRouter()


@dataclass(frozen=True)
class AgentGitSwapDependencies:
    store: Any
    require_internal: Callable[[Request], Awaitable[None]]


def get_agent_git_swap_dependencies(
    request: Request,
) -> AgentGitSwapDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.agent_git_swap_dependencies_factory()


class DriverStateBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lease_token: str = Field(..., max_length=128)


# A literal (scripts/check_endpoint_auth.py reads it): the agent's
# ``DRIVER_STATE_PATH``, which a test holds equal.
@router.post("/api/agents/git-swap/driver-state")
async def agent_git_swap_driver_state(
    request: Request, body: DriverStateBody
) -> JSONResponse:
    """Whether the driver pod of the binding this lease token names was
    refused: ``refused`` (with a fixed reason), ``waiting`` or
    ``unknown``."""
    dependencies = get_agent_git_swap_dependencies(request)
    await dependencies.require_internal(request)
    async with dependencies.store.acquire() as conn:
        state = await binding_driver_state(conn, body.lease_token)
    return JSONResponse(state, headers=NO_STORE)


__all__ = [
    "AgentGitSwapDependencies",
    "agent_git_swap_driver_state",
    "get_agent_git_swap_dependencies",
    "router",
]
