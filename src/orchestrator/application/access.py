"""Request gates composed from security primitives and application state.

The admin gate and the LLM-readiness gate are reused by several domains'
dependencies; each is bound per application (``functools.partial(gate,
resources)``) where a dependency object needs it.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, Request

from orchestrator.application.resources import ApplicationResources
from orchestrator.security import access, auth
from orchestrator.services import readiness as readiness_service

logger = logging.getLogger(__name__)


async def enforce_readiness_gate(resources: ApplicationResources) -> None:
    """Raise 503 when the LLM stack isn't ready.

    Called from ``POST /api/jobs`` and ``POST /api/persistent/threads``
    so dispatch hard-fails rather than silently routing to a chat model
    that doesn't exist. The error body carries the same ``missing_*``
    fields the cockpit reads from ``/api/system/readiness`` so the UI
    can deep-link to the right admin page from either source.
    """
    readiness = await readiness_service.compute_readiness(resources.postgres_db)
    if readiness.get("ready"):
        return
    raise HTTPException(
        status_code=503,
        detail=readiness_service.gate_error_detail(readiness),
    )


async def require_admin(
    resources: ApplicationResources, request: Request
) -> dict[str, Any]:
    """Retain call-time composition bindings for remaining main-module routes."""
    return await access.require_admin(
        request,
        resources.postgres_db,
        resolve_user=auth.require_approved_user,
        audit=access.log_security_event,
    )
