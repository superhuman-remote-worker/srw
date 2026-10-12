"""Request models for owner session input and interrupt (R1.B10)."""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ThreadInputRequest(BaseModel):
    """Body for POST /api/persistent/threads/{thread_id}/input."""

    content: str
    turn_id: Optional[int] = None
    expected_conversation_revision: int | None = Field(default=None, ge=0)


class ThreadInterruptRequest(BaseModel):
    """Optional correlated envelope; an empty body is pinned back-compat."""

    model_config = ConfigDict(extra="forbid")

    client_request_id: UUID | None = None
    target_turn_id: int | None = Field(
        default=None,
        ge=1,
        le=2_147_483_647,
        strict=True,
    )

    @model_validator(mode="after")
    def validate_complete_envelope(self) -> "ThreadInterruptRequest":
        if (self.client_request_id is None) != (self.target_turn_id is None):
            raise ValueError(
                "client_request_id and target_turn_id must be supplied together"
            )
        return self


__all__ = ["ThreadInputRequest", "ThreadInterruptRequest"]
