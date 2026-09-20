"""Authenticated cleanup reservations for controller-owned disk mutation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from orchestrator.services.vm_lifecycle_auth import (
    AUTH_FIELD,
    configured_secret,
    sign_payload,
    unsigned_payload,
    verify_payload,
)
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest


router = APIRouter(prefix="/api/internal/vm-workspace-cleanup-authority")
_store_factory: Callable[[], Any] | None = None
_SOURCES = frozenset(
    {
        "controller_failed_dv_recreate",
        "controller_rootdisk_adopt",
        "controller_rootdisk_delete",
    }
)


def configure(*, store_factory: Callable[[], Any]) -> None:
    global _store_factory
    _store_factory = store_factory


def _response(
    payload: Mapping[str, Any],
    *,
    operation: str,
    correlation_id: str,
    status: int = 200,
) -> JSONResponse:
    return JSONResponse(
        sign_payload(
            payload,
            direction="response",
            operation=operation,
            secret=configured_secret(),
            correlation_id=correlation_id,
        ),
        status_code=status,
    )


async def require_vm_cleanup_authority(
    request: Request, *, operation: str
) -> tuple[dict[str, Any], str]:
    value = await request.json()
    secret = configured_secret()
    if (
        not isinstance(value, Mapping)
        or secret is None
        or not verify_payload(
            value,
            direction="request",
            operation=operation,
            secret=secret,
        )
    ):
        raise ValueError("unauthenticated cleanup authority request")
    auth = value.get(AUTH_FIELD)
    correlation_id = auth.get("request_id") if isinstance(auth, Mapping) else None
    if not isinstance(correlation_id, str) or not correlation_id:
        raise ValueError("cleanup authority request has no correlation ID")
    return dict(unsigned_payload(value)), correlation_id


@router.post("/acquire")
async def acquire(request: Request) -> JSONResponse:
    operation = "recovery-cleanup-acquire"
    try:
        payload, correlation_id = await require_vm_cleanup_authority(
            request, operation=operation
        )
    except (ValueError, TypeError, json.JSONDecodeError):
        return JSONResponse({"detail": "unauthenticated"}, status_code=401)
    try:
        owner_kind = payload["owner_kind"]
        source = payload["source"]
        if owner_kind not in {"job", "thread"} or source not in _SOURCES:
            raise ValueError("invalid cleanup authority scope")
        owner_id = UUID(str(payload["owner_id"]))
        pvc_uid = UUID(str(payload["pvc_uid"]))
        request_id = UUID(str(payload["request_id"]))
        dv_uid = str(payload["dv_uid"])
        generation = str(payload.get("provision_generation") or "")
        if not dv_uid or not generation:
            raise ValueError("cleanup identity is incomplete")
        if _store_factory is None:
            raise RuntimeError("cleanup authority store is unavailable")
        digest = cleanup_intent_digest(
            {
                "owner_kind": owner_kind,
                "owner_id": str(owner_id),
                "pvc_uid": str(pvc_uid),
                "dv_uid": dv_uid,
                "provision_generation": generation,
                "source": source,
            }
        )
        permit = await _store_factory().acquire_cleanup_permit(
            owner_kind=owner_kind,
            owner_id=owner_id,
            pvc_uid=pvc_uid,
            request_id=request_id,
            source=source,
            intent_digest=digest,
            revalidate_completed=True,
            **(
                {
                    "parent_cleanup": payload["parent_cleanup"],
                    "parent_provision_generation": payload.get(
                        "parent_provision_generation"
                    ),
                    "expected_vm_uid": payload.get("expected_vm_uid"),
                }
                if payload.get("parent_cleanup") is not None
                else {}
            ),
        )
        return _response(
            {
                "allowed": permit.allowed,
                "admission_id": (
                    str(permit.admission_id)
                    if permit.admission_id is not None
                    else None
                ),
                "recovery_id": (
                    str(permit.recovery_id) if permit.recovery_id is not None else None
                ),
                "reason": permit.reason,
                "completed_outcome": permit.completed_outcome,
                "request_id": str(request_id),
                "intent_digest": digest,
            },
            operation=operation,
            correlation_id=correlation_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _response(
            {"allowed": False, "reason": str(exc)},
            operation=operation,
            correlation_id=correlation_id,
            status=400,
        )
    except Exception:
        return _response(
            {"allowed": False, "reason": "cleanup authority unavailable"},
            operation=operation,
            correlation_id=correlation_id,
            status=503,
        )


@router.post("/complete")
async def complete(request: Request) -> JSONResponse:
    operation = "recovery-cleanup-complete"
    try:
        payload, correlation_id = await require_vm_cleanup_authority(
            request, operation=operation
        )
    except (ValueError, TypeError, json.JSONDecodeError):
        return JSONResponse({"detail": "unauthenticated"}, status_code=401)
    try:
        admission_id = UUID(str(payload["admission_id"]))
        request_id = UUID(str(payload["request_id"]))
        intent_digest = str(payload["intent_digest"])
        outcome = str(payload["outcome"])
        if outcome not in {"adopted", "deleted", "recreated"} or not intent_digest:
            raise ValueError("invalid cleanup outcome")
        if _store_factory is None:
            raise RuntimeError("cleanup authority store is unavailable")
        changed = await _store_factory().complete_cleanup_permit(
            admission_id,
            outcome=outcome,
            request_id=request_id,
            intent_digest=intent_digest,
        )
        return _response(
            {"completed": changed, "admission_id": str(admission_id)},
            operation=operation,
            correlation_id=correlation_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _response(
            {"completed": False, "reason": str(exc)},
            operation=operation,
            correlation_id=correlation_id,
            status=400,
        )
    except Exception:
        return _response(
            {"completed": False, "reason": "cleanup authority unavailable"},
            operation=operation,
            correlation_id=correlation_id,
            status=503,
        )


@router.post("/resume")
async def resume(request: Request) -> JSONResponse:
    operation = "recovery-cleanup-resume"
    try:
        payload, correlation_id = await require_vm_cleanup_authority(
            request, operation=operation
        )
    except (ValueError, TypeError, json.JSONDecodeError):
        return JSONResponse({"detail": "unauthenticated"}, status_code=401)
    try:
        admission_id = UUID(str(payload["admission_id"]))
        request_id = UUID(str(payload["request_id"]))
        intent_digest = str(payload["intent_digest"])
        owner_kind = payload["owner_kind"]
        source = payload["source"]
        if (
            owner_kind not in {"job", "thread"}
            or source not in _SOURCES
            or not intent_digest
        ):
            raise ValueError("invalid cleanup authority scope")
        owner_id = UUID(str(payload["owner_id"]))
        if _store_factory is None:
            raise RuntimeError("cleanup authority store is unavailable")
        permit = await _store_factory().resume_cleanup_permit(
            admission_id,
            owner_kind=owner_kind,
            owner_id=owner_id,
            source=source,
            request_id=request_id,
            intent_digest=intent_digest,
        )
        return _response(
            {
                "allowed": permit.allowed,
                "admission_id": str(admission_id),
                "reason": permit.reason,
                "completed_outcome": permit.completed_outcome,
            },
            operation=operation,
            correlation_id=correlation_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        return _response(
            {"allowed": False, "reason": str(exc)},
            operation=operation,
            correlation_id=correlation_id,
            status=400,
        )
    except Exception:
        return _response(
            {"allowed": False, "reason": "cleanup authority unavailable"},
            operation=operation,
            correlation_id=correlation_id,
            status=503,
        )
