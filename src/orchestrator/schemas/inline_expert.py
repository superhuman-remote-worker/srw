"""The inline Expert a creation request may carry instead of a catalogue selector.

The shape is the manifest standard's ``{"inline": ExpertSpec}`` with the
``srw/v1`` runtime (``src/shared/manifests/schema.json`` ``ExpertSelection``),
restricted to what the SRW adapter honours on these endpoints. A creation form
sends it when the user changed a template: it is the complete authored
definition, never a delta over a selected Expert. Structure is validated here;
the installed-image check, the authored-fragment gates and the prompt allowlist
run at admission (``orchestrator.services.inline_expert``), where the trusted
image is known.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class InlineSrwPrivateConfig(BaseModel):
    """The SRW adapter's private settings: the Expert's authored definition."""

    model_config = ConfigDict(extra="forbid")

    config_name: str | None = Field(
        None,
        description=(
            "Base profile the definition is layered over. Omit for the role's "
            "base (worker_base for a job, session_base for a session)."
        ),
    )
    asset_name: str | None = Field(
        None,
        description=(
            "Installed asset directory that supplies prompt, matrix and skill "
            "files (for example 'developer'). Never configuration."
        ),
    )
    config: dict[str, Any] = Field(
        default_factory=dict, description="The authored configuration fragment."
    )
    prompts: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Authored prompt segments: persona, instructions, strategic, "
            "tactical, summarization."
        ),
    )
    layers: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Further authored fragments, merged in order over config.",
    )


class InlineExpertRuntime(BaseModel):
    """An ``srw/v1`` runtime. The launch envelope stays installation-managed."""

    model_config = ConfigDict(extra="forbid")

    adapter: Literal["srw/v1"]
    image: str | None = Field(
        None,
        description=(
            "Omit to follow the installed harness. An explicit image must equal "
            "the installed one; any other image is refused."
        ),
    )
    config: InlineSrwPrivateConfig = Field(default_factory=InlineSrwPrivateConfig)


class InlineWorkspacePreference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["none", "virtual", "sandbox", "vm"]


class InlineExpertSpec(BaseModel):
    """``ExpertSpec`` for the SRW adapter."""

    model_config = ConfigDict(extra="forbid")

    runtime: InlineExpertRuntime
    workspacePreference: InlineWorkspacePreference | None = Field(
        None,
        description=(
            "Advisory recommendation for creation clients. Admission never "
            "reads it; select the workspace with the request's own field."
        ),
    )


class InlineExpertSelection(BaseModel):
    """``{"inline": ExpertSpec}``: a complete Expert with no catalogue identity."""

    model_config = ConfigDict(extra="forbid")

    inline: InlineExpertSpec


EXPERT_BASED_ON_DESCRIPTION = (
    "Display-only provenance for an inline expert: the catalogue id it was "
    "copied from. Stored with the work, never used for resolution. Ignored "
    "when expert is a catalogue selector."
)


__all__ = [
    "EXPERT_BASED_ON_DESCRIPTION",
    "InlineExpertRuntime",
    "InlineExpertSelection",
    "InlineExpertSpec",
    "InlineSrwPrivateConfig",
    "InlineWorkspacePreference",
]
