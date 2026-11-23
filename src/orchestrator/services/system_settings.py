"""Simple admin system settings: the VM-workspaces and TTS-library switches,
and the default for MCP server questions.

Two admin toggles that share a shape — a ``system_settings`` row whose ``value``
JSONB carries a single ``enabled`` flag — but deliberately *not* a default.
``vm_workspaces`` is fail-open (an absent row reads as enabled, so an upgrade
does not silently disable a working feature); ``tts.elevenlabs_library_enabled``
is fail-closed (an absent row reads as disabled, because an add consumes a
shared plan-limited voice slot). Keeping both here makes the asymmetry visible
instead of accidental.

The responses carry ``enabled``/``updated_at``/``updated_by`` only. There is no
secret in either row, and the store error path reports ``str(e)`` exactly as it
did before — these rows contain no credential material for it to leak.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import HTTPException

#: ``system_settings`` keys owned by this module.
VM_WORKSPACES_SETTING_KEY = "vm_workspaces"
TTS_LIBRARY_SETTING_KEY = "tts.elevenlabs_library_enabled"


class SystemSettingsStore(Protocol):
    def get_system_setting(self, key: str) -> Awaitable[Mapping[str, Any] | None]: ...

    def upsert_system_setting(
        self, key: str, value: Mapping[str, Any], *, updated_by: str
    ) -> Awaitable[Mapping[str, Any] | None]: ...


@dataclass(frozen=True)
class SystemSettingsDependencies:
    store: SystemSettingsStore


def _actor(admin: Mapping[str, Any]) -> str:
    return admin.get("email") or str(admin.get("id", ""))


def _require_enabled_bool(body: Mapping[str, Any]) -> bool:
    enabled = body.get("enabled")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="`enabled` must be a boolean")
    return enabled


def vm_workspaces_response(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """Shape the vm_workspaces system_settings row for API responses."""
    value = (row or {}).get("value") or {}
    enabled = True
    if isinstance(value, dict) and value.get("enabled") is False:
        enabled = False
    updated_at = (row or {}).get("updated_at")
    return {
        "enabled": enabled,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "updated_by": (row or {}).get("updated_by"),
    }


async def get_vm_workspaces_settings(
    *, dependencies: SystemSettingsDependencies
) -> dict[str, Any]:
    """Return the global VM-workspaces kill-switch.

    Absent row is reported as enabled (fail-open default).
    """
    try:
        row = await dependencies.store.get_system_setting(VM_WORKSPACES_SETTING_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return vm_workspaces_response(row)


async def put_vm_workspaces_settings(
    *,
    body: Mapping[str, Any],
    admin: Mapping[str, Any],
    dependencies: SystemSettingsDependencies,
) -> dict[str, Any]:
    """Toggle the global VM-workspaces kill-switch.

    When disabled, every VM workspace request is denied, including those from
    admin users. Already-running VMs are not torn down — the switch only blocks
    new dispatches.
    """
    enabled = _require_enabled_bool(body)
    try:
        row = await dependencies.store.upsert_system_setting(
            VM_WORKSPACES_SETTING_KEY,
            {"enabled": enabled},
            updated_by=_actor(admin),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return vm_workspaces_response(row)


def tts_library_response(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """Shape the ``tts.elevenlabs_library_enabled`` system_settings row for API
    responses. Absent row → disabled (fail-closed default)."""
    value = (row or {}).get("value") or {}
    enabled = bool(isinstance(value, dict) and value.get("enabled") is True)
    updated_at = (row or {}).get("updated_at")
    return {
        "enabled": enabled,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "updated_by": (row or {}).get("updated_by"),
    }


async def get_tts_library_settings(
    *, dependencies: SystemSettingsDependencies
) -> dict[str, Any]:
    """Return the ElevenLabs Voice Library add gate. Absent row is reported as
    disabled (fail-closed default)."""
    try:
        row = await dependencies.store.get_system_setting(TTS_LIBRARY_SETTING_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return tts_library_response(row)


async def put_tts_library_settings(
    *,
    body: Mapping[str, Any],
    admin: Mapping[str, Any],
    dependencies: SystemSettingsDependencies,
) -> dict[str, Any]:
    """Toggle the ElevenLabs Voice Library add gate. When disabled (the default),
    ``POST /api/settings/tts/library/add`` is refused for everyone; browsing and
    previewing the library stay available regardless. Adds consume the shared
    deployment account's plan-limited voice slots, hence the gate."""
    enabled = _require_enabled_bool(body)
    try:
        row = await dependencies.store.upsert_system_setting(
            TTS_LIBRARY_SETTING_KEY,
            {"enabled": enabled},
            updated_by=_actor(admin),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return tts_library_response(row)


def mcp_server_questions_response(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """Shape the MCP server-questions default for API responses. Absent row
    → ``decline`` (``shared.mcp_questions.FACTORY_DEFAULT``)."""
    from shared.mcp_questions import FACTORY_DEFAULT, setting_or_none

    value = (row or {}).get("value") or {}
    default = setting_or_none(value.get("default")) if isinstance(value, dict) else None
    updated_at = (row or {}).get("updated_at")
    return {
        "default": default or FACTORY_DEFAULT,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "updated_by": (row or {}).get("updated_by"),
    }


async def get_mcp_server_questions_settings(
    *, dependencies: SystemSettingsDependencies
) -> dict[str, Any]:
    """What an MCP connector without its own choice does when its server asks
    a yes/no question mid-call: ``decline`` (the default) or ``accept``."""
    from shared.mcp_questions import SYSTEM_SETTING_KEY

    try:
        row = await dependencies.store.get_system_setting(SYSTEM_SETTING_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return mcp_server_questions_response(row)


async def put_mcp_server_questions_settings(
    *,
    body: Mapping[str, Any],
    admin: Mapping[str, Any],
    dependencies: SystemSettingsDependencies,
) -> dict[str, Any]:
    """Set the installation's default for MCP server questions. It applies at
    the next dispatch of every connector that has no setting of its own."""
    from shared.mcp_questions import SETTINGS, SYSTEM_SETTING_KEY

    default = body.get("default")
    if default not in SETTINGS:
        raise HTTPException(
            status_code=400,
            detail=f"`default` must be one of {', '.join(SETTINGS)}",
        )
    try:
        row = await dependencies.store.upsert_system_setting(
            SYSTEM_SETTING_KEY,
            {"default": default},
            updated_by=_actor(admin),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return mcp_server_questions_response(row)


def image_generation_settings_response(
    row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Shape the image generation settings for API responses. Absent row →
    the defaults (``shared.image_generation``)."""
    from shared.image_generation import normalize_settings

    updated_at = (row or {}).get("updated_at")
    return {
        **normalize_settings((row or {}).get("value")),
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "updated_by": (row or {}).get("updated_by"),
    }


async def get_image_generation_settings(
    *, dependencies: SystemSettingsDependencies
) -> dict[str, Any]:
    """The installation's image generation limits: the paid images one job or
    Session may make (``max_paid_images``, 0 = no cap) and the subscription
    quota share above which the proxy lane refuses new images
    (``proxy_quota_threshold_percent``)."""
    from shared.image_generation import SYSTEM_SETTING_KEY

    try:
        row = await dependencies.store.get_system_setting(SYSTEM_SETTING_KEY)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return image_generation_settings_response(row)


async def put_image_generation_settings(
    *,
    body: Mapping[str, Any],
    admin: Mapping[str, Any],
    dependencies: SystemSettingsDependencies,
) -> dict[str, Any]:
    """Change the image generation limits. Fields left out keep their value;
    applies at the next dispatch."""
    from shared.image_generation import (
        SYSTEM_SETTING_KEY,
        normalize_settings,
        settings_problem,
    )

    problem = settings_problem(body)
    if problem:
        raise HTTPException(status_code=400, detail=problem)
    try:
        current = await dependencies.store.get_system_setting(SYSTEM_SETTING_KEY)
        merged = {**normalize_settings((current or {}).get("value")), **dict(body)}
        row = await dependencies.store.upsert_system_setting(
            SYSTEM_SETTING_KEY,
            normalize_settings(merged),
            updated_by=_actor(admin),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return image_generation_settings_response(row)
