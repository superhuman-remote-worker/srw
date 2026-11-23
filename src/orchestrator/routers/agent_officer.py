"""``/api/agents/threads/{thread_id}/officer/...`` — the Officer's own routes.

Extracted from ``orchestrator.main`` (R1.B07 lane O). Two internal route
declarations, moved with their handler names, paths, methods, parameter order
and docstrings intact. Neither carried ``tags``, ``response_model``,
``status_code`` or a ``dependencies`` list, and neither acquires one here; the
internal-key gate is called in the declaration body exactly where it ran
before.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.schemas.officer_post import OfficerNotifyRequest, OfficerWakeRequest
from orchestrator.security.access import require_internal
from orchestrator.services import officer_paging

router = APIRouter()


def get_officer_paging_dependencies(
    request: Request,
) -> officer_paging.OfficerPagingDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.officer_paging_dependencies_factory()


@router.post("/api/agents/threads/{thread_id}/officer/wake")
async def agent_file_officer_wake(
    request: Request,
    thread_id: str,
    body: OfficerWakeRequest,
) -> dict[str, Any]:
    """File an officer session's durable sleep timer. **Internal** — requires
    ``X-Internal-Key``; ingress strips this path.

    Called by the sleep tool's park path: the timer is a Postgres
    ``session_wake_events`` row (source='timer'), so pod or node death never
    loses the schedule — the drain fires it when due. Minutes are clamped to
    the thread's officer bounds HERE; the tool's value is a request, not an
    order.
    """
    # Design: knowledge-base/knowledge/features/centurion.md §4 (decision 2026-07-29).
    await require_internal(request)
    return await officer_paging.agent_file_officer_wake(
        request,
        thread_id,
        body,
        dependencies=get_officer_paging_dependencies(request),
    )


@router.post("/api/agents/threads/{thread_id}/officer/notify")
async def agent_officer_notify(
    request: Request,
    thread_id: str,
    body: OfficerNotifyRequest,
) -> dict[str, Any]:
    """The officer's notify_user contract. **Internal** — requires
    ``X-Internal-Key``; ingress strips this path.

    Three urgencies, each a feed row on the Legate's notification center
    (unified notification system):
      * ``log`` — no-op server-side: the officer's transcript already carries
        the line; this exists so the tool has an honest cheap tier.
      * ``digest`` — a ``low``-severity row: in-app only, read at the next
        look. The officer card lists these rows (feed filtered by source).
      * ``page`` — a ``high``-severity row: reaches the Legate now, through
        whatever channels their preferences allow. There is no per-officer
        page budget — the platform throttles (dedup per text per day,
        preferences, quiet hours), not the agent.
    """
    # Design: knowledge-base/knowledge/features/centurion.md §6.
    await require_internal(request)
    return await officer_paging.agent_officer_notify(
        request,
        thread_id,
        body,
        dependencies=get_officer_paging_dependencies(request),
    )


__all__ = [
    "agent_file_officer_wake",
    "agent_officer_notify",
    "get_officer_paging_dependencies",
    "router",
]
