"""Live session HTTP adapters; knowledge-base/knowledge/features/live_session_jobs.md §9.1.

- the public webhook of a live connector (the provider's incoming session);
- the cockpit's read side: a job's sessions, their journal and its stream;
- the job's voice subagent's journal (internal key only).

A live connector's pod reaches the orchestrator on the lease exchange port
(``routers/connector_lease_exchange.py``), never here.
"""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.security.access import require_internal, require_job_access
from orchestrator.services.live.inbound import LiveInboundDependencies, receive_webhook
from orchestrator.services.live.store import (
    MAX_EVENTS_PER_WRITE,
    LiveSessionStore,
    session_view,
)
from orchestrator.services.live.stream import session_stream

router = APIRouter()


@dataclass(frozen=True)
class LiveDependencies:
    store: LiveSessionStore
    inbound: LiveInboundDependencies
    require_internal: Callable[..., Awaitable[Any]] = require_internal
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


async def get_dependencies(request: Request) -> LiveDependencies:
    return request.app.state.live_dependencies_factory()


EventKind = Literal[
    "transcript",
    "delegation",
    "answer",
    "instruction",
    "thinking",
    "fallback",
    "question",
    "follow_up",
    "transfer",
    "hangup",
    "dtmf",
    "closed",
    "error",
    "system",
]


class EventWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str | None = Field(None, max_length=200)
    kind: EventKind
    speaker: str | None = Field(None, max_length=40)
    text: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    delegation_id: str | None = Field(None, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)


class EventsWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[EventWrite] = Field(max_length=MAX_EVENTS_PER_WRITE)


async def _session(store: LiveSessionStore, session_id: UUID, job_id=None) -> dict:
    session = await store.get(session_id, job_id=job_id)
    if session is None:
        raise HTTPException(404, "Live session not found")
    return session


def _page(events: list[dict], after_id: int) -> dict:
    return {"events": events, "next_after_id": events[-1]["id"] if events else after_id}


# nosec: public Standard Webhooks signature and timestamp verified inside the handler against the connector's webhook_secret.
@router.post("/api/live/webhooks/{connector_id}")
async def live_webhook(
    connector_id: str,
    request: Request,
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    return await receive_webhook(
        connector_id, request.headers, await request.body(), dependencies.inbound
    )


@router.get("/api/jobs/{job_id}/live-sessions")
async def job_live_sessions(
    job_id: UUID,
    request: Request,
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    return {"sessions": await dependencies.store.job_sessions(job_id)}


@router.get("/api/jobs/{job_id}/live-sessions/{session_id}/events")
async def job_live_session_events(
    job_id: UUID,
    session_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=500),
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    await _session(dependencies.store, session_id, job_id)
    return _page(await dependencies.store.events(session_id, after_id, limit), after_id)


@router.get("/api/jobs/{job_id}/live-sessions/{session_id}/stream")
async def job_live_session_stream(
    job_id: UUID,
    session_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    await _session(dependencies.store, session_id, job_id)
    return StreamingResponse(
        session_stream(
            dependencies.store,
            session_id,
            job_id,
            after_id,
            request,
            dependencies.require_job_access,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/internal/live/sessions/{session_id}/events")
async def internal_live_events(
    session_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=500),
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    await dependencies.require_internal(request)
    session = await _session(dependencies.store, session_id)
    return {
        **_page(await dependencies.store.events(session_id, after_id, limit), after_id),
        "session": session_view(session),
    }


@router.post("/api/internal/live/sessions/{session_id}/events")
async def internal_live_write(
    session_id: UUID,
    request: Request,
    body: EventsWrite,
    dependencies: LiveDependencies = Depends(get_dependencies),
):
    await dependencies.require_internal(request)
    try:
        count = await dependencies.store.write_events(
            session_id, [event.model_dump() for event in body.events]
        )
    except LookupError as exc:
        raise HTTPException(404, "Live session not found") from exc
    return {"inserted": count}
