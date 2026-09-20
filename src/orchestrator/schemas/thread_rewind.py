"""Strict wire contracts for idle stateless conversation rewind."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _canonical_bigint(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise ValueError("must be a canonical nonnegative decimal string")
    if len(value) > 1 and value.startswith("0"):
        raise ValueError("must not contain leading zeroes")
    # PostgreSQL signed bigint range. Sequences are nonnegative in this API.
    parsed = int(value)
    if parsed > 9_223_372_036_854_775_807:
        raise ValueError("is outside the PostgreSQL bigint range")
    return value


class RewindExpected(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_runtime_generation: UUID
    conversation_revision: int = Field(ge=0)
    events_epoch: int = Field(ge=0)
    transcript_tail_seq: str
    input_seq: str | None
    consumed_seq: str | None

    _tail = field_validator("transcript_tail_seq")(_canonical_bigint)
    _watermarks = field_validator("input_seq", "consumed_seq")(_canonical_bigint)


class StatelessRewindRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    client_request_id: UUID
    message_id: UUID
    mode: Literal["conversation"]
    expected: RewindExpected


class StatelessRewindResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: Literal["applied"] = "applied"
    rewind_id: UUID
    client_request_id: UUID
    message_id: UUID
    mode: Literal["conversation"] = "conversation"
    prompt: str
    swept_count: int = Field(ge=1)
    surviving_turn: int = Field(ge=0)
    conversation_revision: int = Field(ge=1)
    events_epoch: int = Field(ge=0)
    event_seq: str

    _event_seq = field_validator("event_seq")(_canonical_bigint)


class StatelessRewindPostResponse(StatelessRewindResult):
    duplicate: bool


class StatelessRewindPreview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: UUID
    mode: Literal["conversation"] = "conversation"
    prompt: str
    eligible: bool
    refusal_code: str | None
    swept_count: int = Field(ge=0)
    expected: RewindExpected | None


__all__ = [
    "RewindExpected",
    "StatelessRewindPostResponse",
    "StatelessRewindPreview",
    "StatelessRewindRequest",
    "StatelessRewindResult",
]
