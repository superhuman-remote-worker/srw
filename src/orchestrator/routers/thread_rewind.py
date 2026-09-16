"""Owner-facing REST endpoints for idle stateless conversation rewind."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from orchestrator.schemas.thread_rewind import (
    StatelessRewindPostResponse,
    StatelessRewindPreview,
    StatelessRewindRequest,
    StatelessRewindResult,
)
from orchestrator.services.thread_rewind import RewindFailure, ThreadRewindService


router = APIRouter(prefix="/api/persistent/threads", tags=["Thread rewind"])


@dataclass(frozen=True, slots=True)
class ThreadRewindDependencies:
    store: Any
    service: ThreadRewindService
    require_thread_owner: Callable[
        [Request, Any, str], Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]


def get_thread_rewind_dependencies(request: Request) -> ThreadRewindDependencies:
    return request.app.state.thread_rewind_dependencies_factory()


def _failure(exc: RewindFailure) -> HTTPException:
    return HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "reason": exc.reason},
        headers={"Cache-Control": "private, no-store"},
    )


def _private_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "private, no-store"


@router.get(
    "/{thread_id}/rewinds/preview",
    response_model=StatelessRewindPreview,
)
async def preview_stateless_rewind(
    thread_id: str,
    message_id: UUID,
    request: Request,
    response: Response,
    dependencies: ThreadRewindDependencies = Depends(get_thread_rewind_dependencies),
) -> StatelessRewindPreview:
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    _private_no_store(response)
    try:
        return await dependencies.service.preview(thread_id, message_id, user)
    except RewindFailure as exc:
        raise _failure(exc) from exc


@router.post(
    "/{thread_id}/rewinds",
    response_model=StatelessRewindPostResponse,
)
async def apply_stateless_rewind(
    thread_id: str,
    body: StatelessRewindRequest,
    request: Request,
    response: Response,
    dependencies: ThreadRewindDependencies = Depends(get_thread_rewind_dependencies),
) -> StatelessRewindPostResponse:
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    _private_no_store(response)
    try:
        result, duplicate = await dependencies.service.apply(thread_id, body, user)
    except RewindFailure as exc:
        raise _failure(exc) from exc
    return StatelessRewindPostResponse(
        **result.model_dump(),
        duplicate=duplicate,
    )


@router.get(
    "/{thread_id}/rewinds/by-client-request/{client_request_id}",
    response_model=StatelessRewindResult,
)
async def get_stateless_rewind_receipt(
    thread_id: str,
    client_request_id: UUID,
    request: Request,
    response: Response,
    dependencies: ThreadRewindDependencies = Depends(get_thread_rewind_dependencies),
) -> StatelessRewindResult:
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    _private_no_store(response)
    try:
        return await dependencies.service.receipt(thread_id, client_request_id, user)
    except RewindFailure as exc:
        raise _failure(exc) from exc


__all__ = [
    "ThreadRewindDependencies",
    "get_thread_rewind_dependencies",
    "router",
]
