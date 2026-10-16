"""Request models for the owner-facing session surface (R1.B10)."""

from __future__ import annotations

from typing import Any, Literal, Optional, get_args
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from orchestrator.schemas.inline_expert import InlineExpertSelection


# The `threads.valid_permission_mode` CHECK values. Request schemas validate
# against this so a bad value is a 422, not a 500 from the insert.
PermissionMode = Literal["supervised", "auto_accept", "autonomous"]
PERMISSION_MODES = frozenset(get_args(PermissionMode))


class ThreadControlRequest(BaseModel):
    """Strict public envelope for the durable control-inbox subset."""

    client_request_id: UUID
    method: Literal[
        "mode.set", "narration.set", "workspace.undo", "compact", "upgrade-to-workspace"
    ]
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
    focus: str | None = Field(
        None,
        max_length=4000,
        description="compact only: optional focus for the summary (/compact <focus>).",
    )
    boundary_message_id: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description=(
            "compact only: summarize everything before this transcript "
            "message, keeping it and what follows (the rewind sheet's "
            "'Summarize up to here')."
        ),
    )
    target_tier: Literal["sandbox", "container", "vm"] | None = Field(
        None,
        description=(
            "upgrade-to-workspace only: the tier to move to; omitted means the "
            "next tier (a container for a stateless Virtual/None session)."
        ),
    )
    template: str | None = Field(
        None,
        min_length=1,
        max_length=200,
        description="upgrade-to-workspace only: a WorkspaceTemplate name.",
    )

    @model_validator(mode="after")
    def validate_method_mode_pair(self) -> "ThreadControlRequest":
        narration_modes = {"silent", "verbose", "auto"}
        if self.method == "mode.set" and self.mode not in PERMISSION_MODES:
            raise ValueError("mode.set requires a permission mode")
        if self.method == "narration.set" and self.mode not in narration_modes:
            raise ValueError("narration.set requires a narration mode")
        if self.method == "workspace.undo" and self.mode is not None:
            raise ValueError("workspace.undo does not accept a mode")
        if self.method == "compact" and self.mode is not None:
            raise ValueError("compact does not accept a mode")
        if self.method != "compact" and (
            self.focus is not None or self.boundary_message_id is not None
        ):
            raise ValueError(
                "focus and boundary_message_id are accepted only by compact"
            )
        if self.method == "upgrade-to-workspace":
            if self.mode is not None:
                raise ValueError("upgrade-to-workspace does not accept a mode")
            if self.session_runtime_generation is None:
                raise ValueError(
                    "upgrade-to-workspace requires session_runtime_generation"
                )
        elif self.target_tier is not None or self.template is not None:
            raise ValueError(
                "target_tier and template are accepted only by upgrade-to-workspace"
            )
        return self

    def control_payload(self) -> dict[str, Any]:
        """Canonical payload used for idempotency and durable admission.

        ``compact`` carries only the keys that are present, so an empty focus
        and an absent one are the same request. ``upgrade-to-workspace``
        spells a container as ``sandbox`` and always carries the runtime
        generation it was asked against.
        """

        if self.method == "workspace.undo":
            return {}
        if self.method == "compact":
            payload: dict[str, Any] = {}
            if self.focus and self.focus.strip():
                payload["focus"] = self.focus.strip()
            if self.boundary_message_id:
                payload["boundary_message_id"] = self.boundary_message_id
            return payload
        if self.method == "upgrade-to-workspace":
            payload = {"runtime_generation": str(self.session_runtime_generation)}
            if self.target_tier is not None:
                payload["target_tier"] = (
                    "sandbox" if self.target_tier == "container" else self.target_tier
                )
            if self.template:
                payload["template"] = self.template
            return payload
        return {"mode": self.mode}


class ToolGroupPreviewRequest(BaseModel):
    """What would a session or job created with THIS config bind? (a prediction)"""

    config_name: Optional[str] = None
    expert_id: Optional[str] = None
    #: A complete inline Expert, as the create request would carry it. It is
    #: the expert layer of the prediction; never combined with expert_id or a
    #: non-base config_name.
    expert: Optional[InlineExpertSelection] = None
    project_id: Optional[str] = None
    config_override: Optional[dict[str, Any]] = None
    workspace: Optional[dict[str, Any]] = None
    workspace_preference: Literal["none", "virtual", "sandbox", "vm"] | None = None
    #: Which surface is asking. ``worker`` is the job-create form and defaults
    #: the base to ``worker_base``; ``session`` is the New Session form. Default
    #: stays ``session`` so the shipped cockpit's payloads keep their meaning.
    expert_type: Literal["worker", "session"] = "session"


__all__ = ["ThreadControlRequest", "ToolGroupPreviewRequest"]
