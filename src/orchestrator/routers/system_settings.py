"""HTTP adapters for the simple boolean admin system settings."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.security.access import require_internal
from orchestrator.services import image_generation_quota, system_settings

router = APIRouter()


@dataclass(frozen=True)
class SystemSettingsDependencies:
    operations: system_settings.SystemSettingsDependencies
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]
    # The agent-facing quota read authenticates with the internal key.
    require_internal: Callable[[Request], Awaitable[None]] = require_internal


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


@router.get("/api/admin/system-settings/image_generation")
async def get_image_generation_settings(
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Return the image generation limits: paid images per job or Session
    (``max_paid_images``, 0 = no cap; default 20) and the subscription quota
    threshold for the proxy lane (``proxy_quota_threshold_percent``, default
    80). Admin-only."""
    await dependencies.require_admin(request)
    return await system_settings.get_image_generation_settings(
        dependencies=dependencies.operations
    )


@router.put("/api/admin/system-settings/image_generation")
async def put_image_generation_settings(
    body: dict[str, Any],
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Change the image generation limits. Admin-only; applies at the next
    dispatch (the cap) or the next image call (the quota threshold)."""
    admin = await dependencies.require_admin(request)
    return await system_settings.put_image_generation_settings(
        body=body, admin=admin, dependencies=dependencies.operations
    )


@router.get("/api/internal/image-generation/proxy-quota")
async def get_proxy_image_quota(
    request: Request,
    *,
    dependencies: SystemSettingsDependencies = Depends(
        get_system_settings_dependencies
    ),
) -> dict[str, Any]:
    """Whether the subscription proxy has room for another image. Called by
    the agent's generate_image tool before each proxy image call
    (internal key). Never returns account identity or tokens."""
    await dependencies.require_internal(request)
    return await image_generation_quota.proxy_image_quota(
        store=dependencies.operations.store
    )
