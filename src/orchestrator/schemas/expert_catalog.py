"""Expert and skill catalogue wire schemas and prompt-key validation."""

from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field, field_validator


class ExpertInfo(BaseModel):
    """Expert configuration metadata for discovery."""

    id: str
    display_name: str
    description: str
    icon: str = "psychology"
    color: str = "#cba6f7"
    tags: list[str] = []
    expert_type: Literal["worker", "session"] = "worker"


_ALLOWED_EXPERT_PROMPT_KEYS = {
    "persona",
    "instructions",
    "summarization",
}

#: The editor's old strategic/tactical prompt fields. A worker's phase
#: guidance is the strategic-phase / tactical-phase skills, so these are
#: dropped on every write and copy (an older export, a stored manifest)
#: instead of refused; migration 0567 stripped them from the stored rows.
RETIRED_EXPERT_PROMPT_KEYS = frozenset({"strategic", "tactical"})


def validate_expert_prompts(v: dict[str, Any] | None) -> dict[str, Any] | None:
    """Validate the DB-expert prompt boundary (defense-in-depth)."""
    if v:
        v = {k: val for k, val in v.items() if k not in RETIRED_EXPERT_PROMPT_KEYS}
        unknown = set(v) - _ALLOWED_EXPERT_PROMPT_KEYS
        if unknown:
            raise ValueError(f"Unknown prompt keys: {sorted(unknown)}")
    from shared.runtime.core.expert_resolution import (
        validate_expert_persona_placeholders,
    )

    return validate_expert_persona_placeholders(v)


def validate_expert_prompt_source_or_422(
    prompts: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Reject an invalid stored/bundled copy source as an authoring error."""
    try:
        return validate_expert_prompts(prompts)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


class ExpertCreate(BaseModel):
    """Create a DB-backed expert (Slice 1: hard-deny validated; grants in S2)."""

    name: str = Field(..., pattern=r"^[a-z][a-z0-9_-]*$", max_length=100)
    display_name: str = Field(..., min_length=1, max_length=200)
    expert_type: Literal["worker", "session"]
    description: str | None = None
    icon: str = "smart_toy"
    color: str = Field("#6B7280", pattern=r"^#[0-9A-Fa-f]{6}$")
    tags: list[str] = []
    config: dict[str, Any] = {}
    prompts: dict[str, Any] = {}

    @field_validator("prompts")
    @classmethod
    def _check_prompts(cls, v: dict[str, Any]) -> dict[str, Any]:
        return validate_expert_prompts(v)


class ExpertUpdate(BaseModel):
    """Patch a DB expert; expert_type is immutable (decision 3) so it is absent."""

    display_name: str | None = Field(None, min_length=1, max_length=200)
    description: str | None = None
    icon: str | None = None
    color: str | None = Field(None, pattern=r"^#[0-9A-Fa-f]{6}$")
    tags: list[str] | None = None
    config: dict[str, Any] | None = None
    prompts: dict[str, Any] | None = None

    @field_validator("prompts")
    @classmethod
    def _check_prompts(cls, v: dict[str, Any] | None) -> dict[str, Any] | None:
        return validate_expert_prompts(v)


class SkillInfo(BaseModel):
    """Skill catalog metadata for discovery (the L1 'menu' entry)."""

    id: str
    name: str
    display_name: str
    description: str
    icon: str = "extension"
    color: str = "#6B7280"
    tags: list[str] = []


class SkillCreate(BaseModel):
    """Create a DB-backed skill from its file tree (must include SKILL.md).

    name + description are parsed from SKILL.md frontmatter, not sent separately."""

    files: dict[str, str]
    display_name: str | None = Field(None, max_length=200)
    icon: str = "extension"
    color: str = Field("#6B7280", pattern=r"^#[0-9A-Fa-f]{6}$")
    tags: list[str] = []


class SkillUpdate(BaseModel):
    """Patch a DB skill; name is immutable (derived from SKILL.md) so it is absent."""

    files: dict[str, str] | None = None
    display_name: str | None = Field(None, min_length=1, max_length=200)
    icon: str | None = None
    color: str | None = Field(None, pattern=r"^#[0-9A-Fa-f]{6}$")
    tags: list[str] | None = None
    is_global: bool | None = None


class ExpertDefaultSetRequest(BaseModel):
    expert_id: str


class ExpertDefaultForkRequest(BaseModel):
    expert_id: str | None = None
