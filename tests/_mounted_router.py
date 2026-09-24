"""Isolated HTTP applications for characterizing and extracting existing routes.

Mount real routers so FastAPI retains its normal dependency/response handling,
including lazy router inclusion. These applications never run main's lifespan.
"""

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute


def mount_router(
    *routers: APIRouter,
    factories: Mapping[str, Callable[..., Any]] | None = None,
) -> FastAPI:
    """Mount the given routers with only this application's operation factories."""
    from orchestrator.application.http import CustomJSONResponse

    app = FastAPI(default_response_class=CustomJSONResponse)
    for name, factory in (factories or {}).items():
        setattr(app.state, name, factory)
    for router in routers:
        app.include_router(router)
    return app


def mount_main_routes(paths: Iterable[str]) -> FastAPI:
    """Capture exact pre-extraction route objects for baseline wire cases.

    This deliberately requires direct declarations: after an extraction, cases
    must mount their new owner with explicit factories via ``mount_router``.
    """
    from orchestrator.main import app

    requested = set(paths)
    selected = [
        route
        for route in app.routes
        if isinstance(route, APIRoute) and route.path in requested
    ]
    missing = requested - {route.path for route in selected}
    if missing:
        raise AssertionError(f"No direct main routes for {sorted(missing)}")
    return mount_router(APIRouter(routes=selected))
