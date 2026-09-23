"""Request models for the owner-facing session surface (R1.B10)."""

from __future__ import annotations

from typing import Any, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class ThreadControlRequest(BaseModel):
    """Strict public envelope for the durable control-inbox subset."""

    client_request_id: UUID
    method: Literal["mode.set", "narration.set", "workspace.undo"]
    session_runtime_generation: UUID | None = Field(
        None,
        description=(
            "Runtime generation rendered with the current session. Pinned "
            "admission compares it under the thread-row lock."
        ),
    )
    mode: (
        Literal[
            "supervised",
            "auto_accept",
            "autonomous",
            "silent",
            "verbose",
            "auto",
        ]
        | None
    ) = None

    @model_validator(mode="after")
    def validate_method_mode_pair(self) -> "ThreadControlRequest":
        permission_modes = {"supervised", "auto_accept", "autonomous"}
        narration_modes = {"silent", "verbose", "auto"}
        if self.method == "mode.set" and self.mode not in permission_modes:
            raise ValueError("mode.set requires a permission mode")
        if self.method == "narration.set" and self.mode not in narration_modes:
            raise ValueError("narration.set requires a narration mode")
        if self.method == "workspace.undo" and self.mode is not None:
            raise ValueError("workspace.undo does not accept a mode")
        return self

    def control_payload(self) -> dict[str, Any]:
        """Canonical payload used for idempotency and durable admission."""

        return {} if self.method == "workspace.undo" else {"mode": self.mode}


class ToolGroupPreviewRequest(BaseModel):
    """What would a session or job created with THIS config bind? (a prediction)"""

    config_name: Optional[str] = None
    expert_id: Optional[str] = None
    project_id: Optional[str] = None
    config_override: Optional[dict[str, Any]] = None
    workspace: Optional[dict[str, Any]] = None
    workspace_preference: Literal["none", "virtual", "sandbox", "vm"] | None = None
    #: Which surface is asking. ``worker`` is the job-create form and defaults
    #: the base to ``worker_base``; ``session`` is the New Session form. Default
    #: stays ``session`` so the shipped cockpit's payloads keep their meaning.
    expert_type: Literal["worker", "session"] = "session"


__all__ = ["ThreadControlRequest", "ToolGroupPreviewRequest"]
