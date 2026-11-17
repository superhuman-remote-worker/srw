"""HTTP adapter for what installation a client is talking to."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.security.auth import get_current_user
from orchestrator.services import installation

router = APIRouter()


@dataclass(frozen=True)
class VersionDependencies:
    store: Any
    get_current_user: Callable[..., Awaitable[Any]] = get_current_user
    server_version: Callable[[], dict[str, Any]] = installation.server_version


def get_version_dependencies(request: Request) -> VersionDependencies:
    return request.app.state.version_dependencies_factory()


@router.get("/api/version")
async def server_version(
    request: Request,
    *,
    dependencies: VersionDependencies = Depends(get_version_dependencies),
) -> dict[str, Any]:
    """The release this installation runs and the manifest API versions it accepts.

    For a client that must match its server (the `srw` command line reports a
    version skew from it). Any authenticated caller may read it, approved or
    not; it says nothing about the caller.
    """
    await dependencies.get_current_user(request, dependencies.store)
    return dependencies.server_version()
