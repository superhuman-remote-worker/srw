"""HTTP adapters for the simple boolean admin system settings."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.services import system_settings

router = APIRouter()


@dataclass(frozen=True)
class SystemSettingsDependencies:
    operations: system_settings.SystemSettingsDependencies
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]


def get_system_settings_dependencies(request: Request) -> SystemSettingsDependencies:
    return request.app.state.system_settings_dependencies_factory()


@router.get("/api/admin/system-settings/vm_workspaces")
async def get_vm_workspaces_settings(
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Return the global VM-workspaces kill-switch.

    Admin-only. Absent row is reported as enabled (fail-open default).
    """
    await dependencies.require_admin(request)
    return await system_settings.get_vm_workspaces_settings(
        dependencies=dependencies.operations
    )


@router.put("/api/admin/system-settings/vm_workspaces")
async def put_vm_workspaces_settings(
    body: dict[str, Any],
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Toggle the global VM-workspaces kill-switch.

    Admin-only. When disabled, every VM workspace request is denied,
    including those from admin users. Already-running VMs are not torn
    down — the switch only blocks new dispatches.
    """
    admin = await dependencies.require_admin(request)
    return await system_settings.put_vm_workspaces_settings(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.get("/api/admin/system-settings/tts_library")
async def get_tts_library_settings(
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Return the ElevenLabs Voice Library add gate. Admin-only. Absent row is
    reported as disabled (fail-closed default)."""
    await dependencies.require_admin(request)
    return await system_settings.get_tts_library_settings(
        dependencies=dependencies.operations
    )


@router.put("/api/admin/system-settings/tts_library")
async def put_tts_library_settings(
    body: dict[str, Any],
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Toggle the ElevenLabs Voice Library add gate. Admin-only. When disabled
    (the default), ``POST /api/settings/tts/library/add`` is refused for
    everyone; browsing and previewing the library stay available regardless.
    Adds consume the shared deployment account's plan-limited voice slots, hence
    the gate."""
    admin = await dependencies.require_admin(request)
    return await system_settings.put_tts_library_settings(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.get("/api/admin/system-settings/mcp_server_questions")
async def get_mcp_server_questions_settings(
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Return the default for MCP server questions (decline or accept).
    Admin-only. Absent row is reported as decline."""
    await dependencies.require_admin(request)
    return await system_settings.get_mcp_server_questions_settings(
        dependencies=dependencies.operations
    )


@router.put("/api/admin/system-settings/mcp_server_questions")
async def put_mcp_server_questions_settings(
    body: dict[str, Any],
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Set what an MCP connector without its own choice does when its server
    asks a yes/no question mid-call. Admin-only; applies at the next
    dispatch."""
    admin = await dependencies.require_admin(request)
    return await system_settings.put_mcp_server_questions_settings(
        body=body, admin=admin, dependencies=dependencies.operations
    )
