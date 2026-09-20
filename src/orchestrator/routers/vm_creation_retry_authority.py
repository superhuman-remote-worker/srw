"""Lifecycle-MAC authority for source-bound VM create effects.

Reservation responses never grant Kubernetes actuation. A successful begin-effect
CAS grants one effect; duplicate calls, observer expiry and transport loss cannot
mint another grant for that effect.
"""

from collections.abc import Callable
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from orchestrator.routers.vm_workspace_cleanup_authority import (
    require_vm_cleanup_authority,
    _response,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

router = APIRouter(prefix="/api/internal/vm-creation-retries")
_store_factory: Callable[[], Any] | None = None


def configure(*, store_factory: Callable[[], Any]) -> None:
    global _store_factory
    _store_factory = store_factory


async def _dispatch(request, operation, method):
    try:
        payload, correlation_id = await require_vm_cleanup_authority(
            request, operation=operation
        )
    except (ValueError, TypeError):
        return JSONResponse({"detail": "unauthenticated"}, status_code=401)
    try:
        if _store_factory is None:
            raise RuntimeError("Creation authority unavailable")
        if (
            not isinstance(payload.get("request_id"), str)
            or str(UUID(payload["request_id"])) != payload["request_id"]
        ):
            raise ValueError("Invalid request identity")
        if method in ("authorize_controller", "begin_effect"):
            if (
                not isinstance(payload.get("claim_token"), str)
                or str(UUID(payload["claim_token"])) != payload["claim_token"]
            ):
                raise ValueError("Invalid observer identity")
        result = await getattr(_store_factory(), method)(**payload)
        if method == "authorize_controller":
            result = {**result, "actuation_allowed": False}
        return _response(
            jsonable_encoder(result), operation=operation, correlation_id=correlation_id
        )
    except VMCreationRetryConflict as exc:
        return _response(
            {"allowed": False, "reason": exc.reason},
            operation=operation,
            correlation_id=correlation_id,
        )
    except (ValueError, TypeError, KeyError):
        return _response(
            {"allowed": False, "reason": "invalid_creation_evidence"},
            operation=operation,
            correlation_id=correlation_id,
            status=400,
        )
    except Exception:
        return _response(
            {"allowed": False, "reason": "creation_authority_unavailable"},
            operation=operation,
            correlation_id=correlation_id,
            status=503,
        )


@router.post("/authorize")
async def authorize(request: Request) -> JSONResponse:
    return await _dispatch(request, "creation_retry_authorize", "authorize_controller")


@router.post("/begin-effect")
async def begin_effect(request: Request) -> JSONResponse:
    return await _dispatch(request, "creation_retry_begin_effect", "begin_effect")


@router.post("/observe-effect")
async def observe_effect(request: Request) -> JSONResponse:
    return await _dispatch(request, "creation_retry_observe_effect", "observe_effect")


@router.post("/settle-never-issued")
async def settle_never_issued(request: Request) -> JSONResponse:
    return await _dispatch(
        request, "creation_retry_settle_never_issued", "settle_never_issued"
    )
