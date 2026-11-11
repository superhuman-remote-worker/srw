"""Owner-facing thread End, Resume and detached-rewind HTTP adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import APIRouter, Depends, Request

from orchestrator.schemas.thread_lifecycle import (
    ThreadResumeRequest,
    ThreadRewindRequest,
)

end_router = APIRouter()
resume_router = APIRouter()
rewind_router = APIRouter()
router = APIRouter()


class ThreadRetirementOperations(Protocol):
    async def end_thread_flow(
        self,
        thread_id: str,
        thread: dict[str, Any],
        *,
        permanent: bool,
        force: bool,
        **kwargs: Any,
    ) -> dict[str, Any]: ...


class ThreadResumeOperations(Protocol):
    async def resume_thread(
        self,
        thread_id: str,
        user: dict[str, Any],
        thread: dict[str, Any],
        body: ThreadResumeRequest | None = None,
    ) -> dict[str, Any]: ...

    async def rewind_thread_detached(
        self,
        thread_id: str,
        user: dict[str, Any],
        thread: dict[str, Any],
        body: ThreadRewindRequest,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class ThreadLifecycleRouteDependencies:
    """Application-owned collaborators for one lifecycle request."""

    store: Any
    retirement: ThreadRetirementOperations
    resume: ThreadResumeOperations
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]


def get_thread_lifecycle_dependencies(
    request: Request,
) -> ThreadLifecycleRouteDependencies:
    return request.app.state.thread_lifecycle_dependencies_factory()


@end_router.delete("/api/persistent/threads/{thread_id}")
async def end_thread(
    thread_id: str,
    request: Request,
    permanent: bool = False,
    force: bool = False,
    *,
    dependencies: ThreadLifecycleRouteDependencies = Depends(
        get_thread_lifecycle_dependencies
    ),
) -> dict[str, Any]:
    """End (or permanently delete) a persistent thread (auth: owner only).

    Query params:
        permanent: If true, delete the thread row and all associated messages
                   from the database. If false (default), just mark as ended.
        force: Required to end a session whose agent is mid-turn. Without it
               a live turn returns 409 — a sessions-list cleanup sweep tore
               down an active session mid-turn, destroying its in-memory
               input queue.
    """
    # Incident: knowledge-history/done/session_silent_failure_audit.md #11.
    _user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await dependencies.retirement.end_thread_flow(
        thread_id,
        thread,
        permanent=permanent,
        force=force,
    )


@resume_router.post("/api/persistent/threads/{thread_id}/resume")
async def resume_thread(
    thread_id: str,
    request: Request,
    body: ThreadResumeRequest | None = None,
    *,
    dependencies: ThreadLifecycleRouteDependencies = Depends(
        get_thread_lifecycle_dependencies
    ),
) -> dict[str, Any]:
    """Resume an ended thread (auth: owner only).

    Resets thread status to 'created' and clears the stale agent_id so that
    a new agent can pick it up. The frontend navigates to the chat page after
    calling this, where the orchestrator will provision or wait for an agent.

    Drifted config (deleted/revoked connectors or projects, withdrawn grants)
    is reported as 428 rather than silently denied; the caller re-POSTs with
    ``acknowledge`` naming the drift ids it accepts losing.
    """
    # Design: knowledge-history/done/session_config_drift_resume.md.
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await dependencies.resume.resume_thread(thread_id, user, thread, body)


@rewind_router.post("/api/agents/threads/{thread_id}/rewind")
async def rewind_thread_detached(
    thread_id: str,
    request: Request,
    body: ThreadRewindRequest,
    *,
    dependencies: ThreadLifecycleRouteDependencies = Depends(
        get_thread_lifecycle_dependencies
    ),
) -> dict[str, Any]:
    """Rewind a DETACHED session's transcript (auth: owner only).

    Conversation mode only: file restore needs the agent that holds the
    workspace, so live sessions rewind through the session WebSocket instead,
    and code modes here answer 400 ("resume first"). A bound agent means the
    in-memory authority is live and a DB-only sweep would diverge it → 409.
    """
    # Design: knowledge-history/done/session_rewind.md §Flow — detached.
    user, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await dependencies.resume.rewind_thread_detached(
        thread_id, user, thread, body
    )


router.include_router(end_router)
router.include_router(resume_router)
router.include_router(rewind_router)


__all__ = [
    "ThreadLifecycleRouteDependencies",
    "ThreadResumeOperations",
    "ThreadRetirementOperations",
    "get_thread_lifecycle_dependencies",
    "end_router",
    "resume_router",
    "rewind_router",
    "router",
]
