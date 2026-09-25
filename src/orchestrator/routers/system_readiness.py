"""``GET /api/system/readiness`` — the onboarding screen's readiness signal.

Moved from ``orchestrator.main`` (R1.B12); the path, method, handler name and
therefore the generated operation id are unchanged, and the router is included
where the route used to be declared (after every other router), so route order
is unchanged too.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Request

from orchestrator.security.auth import get_current_user
from orchestrator.services import readiness as readiness_service

router = APIRouter()


@dataclass(frozen=True)
class SystemReadinessDependencies:
    """Built per request by ``app.state.system_readiness_dependencies_factory``."""

    store: Any


# nosec: public auth-bootstrap (Bearer-required, intentionally pre-approval — onboarding first paint)
@router.get("/api/system/readiness")
async def system_readiness(request: Request) -> dict[str, Any]:
    """Return the cockpit-facing readiness signal.

    Authenticated, but not admin-gated — the onboarding screen calls this
    on first paint. Auth-required because the response leaks details
    about whether catalog rows exist (a low-stakes leak, but still
    user-scoped). See ``readiness_service.compute_readiness`` for the
    payload shape.
    """
    dependencies = request.app.state.system_readiness_dependencies_factory()
    await get_current_user(request, dependencies.store)
    return await readiness_service.compute_readiness(dependencies.store)


__all__ = ["SystemReadinessDependencies", "router", "system_readiness"]
