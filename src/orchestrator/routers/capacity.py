"""Admin read of stateless executor capacity — what the KEDA scaler sees.

Design: knowledge-base/knowledge/features/capacity_ux_and_queue_autoscaling.md
§2. Deliberately admin-only: queue depth is operator information, never shown
to end users (the cockpit renders a queued state without any count).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

router = APIRouter()


@dataclass(frozen=True)
class CapacityDependencies:
    snapshot: Callable[[], Awaitable[dict[str, Any]]]
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]
    vm_snapshot: Callable[[], Awaitable[dict[str, Any]]] | None = None


def get_capacity_dependencies(request: Request) -> CapacityDependencies:
    return request.app.state.capacity_dependencies_factory()


@router.get("/api/admin/capacity")
async def get_capacity(
    request: Request,
    *,
    dependencies: CapacityDependencies = Depends(get_capacity_dependencies),
) -> dict[str, Any]:
    """Executor inventory, runnable queue depth, and the scaler's ``desired``.

    Admin-only. ``executors.total``/``ready`` are ``null`` when the Kubernetes
    API is unreachable; the queue numbers always come from ``run_queue``.
    """
    await dependencies.require_admin(request)
    result = await dependencies.snapshot()
    if dependencies.vm_snapshot is not None:
        result = {**result, "vm": await dependencies.vm_snapshot()}
    return result
