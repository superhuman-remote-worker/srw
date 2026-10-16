"""Request models for live session-configuration and workspace-upgrade routes.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_CONFIG``). Class names are the wire contract of four routes and move
unchanged.

``datasource_ids`` carries create semantics on both config models: ``None``
means "no connector change" and ``[]`` means "detach all". That distinction is
authoritative — a caller cannot express "detach all" any other way — so the
field stays ``list[str] | None`` rather than defaulting to an empty list.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class AgentThreadConfigUpdateRequest(BaseModel):
    config_override: dict[str, Any]
    snapshot_patch_protocol: Literal[1] | None = None
    snapshot_generation: int | None = Field(default=None, ge=1)
    # Live datasource change (live_session_settings.md Slice B): the desired
    # FULL selection, matching create semantics. None = no datasource change;
    # [] = detach all.
    datasource_ids: list[str] | None = None
    # The level of connectors with access levels, by id (main cloud slice
    # 5), applied at the next attach; None = no change. Never to or from
    # protected.
    datasource_access: dict[str, str] | None = None


class ThreadWorkspaceUpgradeRequest(BaseModel):
    """Body for ``POST /api/agents/threads/{id}/upgrade-to-workspace``.

    ``target_tier`` None means the next tier up; ``template`` names a template
    the Session's owner can read (Project, Account, then Catalog).
    ``control_request_id`` names the stateless lane switch's durable request
    (C2): the VM request then records the upgrade's seed marker.
    """

    target_tier: str | None = None
    template: str | None = None
    control_request_id: UUID | None = None


class VmUpgradeSeededRequest(BaseModel):
    """Body for ``POST /api/agents/threads/{id}/workspace-upgrade/seeded`` (C2)."""

    provision_generation: UUID
    control_request_id: UUID


class VmUpgradeInterruptedRequest(BaseModel):
    """Body for ``POST /api/agents/threads/{id}/workspace-upgrade/interrupted``
    (C2): the copy's source is permanently gone."""

    provision_generation: UUID
    control_request_id: UUID


class ThreadConfigPatchRequest(BaseModel):
    config_override: dict[str, Any] = Field(default_factory=dict)
    # Desired FULL datasource selection (create semantics): None = no change,
    # [] = detach all. Same key as the internal agent PATCH.
    datasource_ids: list[str] | None = None
    # As the internal agent PATCH: levels by connector id, None = no change.
    datasource_access: dict[str, str] | None = None


__all__ = [
    "AgentThreadConfigUpdateRequest",
    "ThreadConfigPatchRequest",
    "ThreadWorkspaceUpgradeRequest",
    "VmUpgradeInterruptedRequest",
    "VmUpgradeSeededRequest",
]
