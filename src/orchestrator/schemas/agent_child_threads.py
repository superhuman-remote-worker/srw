"""Request bodies for the agent-facing thread and subagent-child surfaces.

Moved verbatim from ``orchestrator.main`` (R1.B06, lane C, census group
``S_CHILD``). Class names, field names, defaults and every ``min_length`` /
``max_length`` / ``ge`` bound are reproduced exactly: they are the published
request contract for the agent runtime, and a relaxed bound here is an
unbounded write on the other side of an internal-key transport.

Two shapes are deliberate rather than accidental:

* ``AgentSubagentThreadCreateRequest.parent_thread_id`` is typed ``None`` — an
  explicit NULL-only compatibility field, not an oversight. A worker child
  belongs solely to the job named in the path; session children use the
  separate ``/api/agents/threads/...`` routes.
* The ``AgentSessionSubagent*`` request bodies inherit from one query base so
  the parent authority can never be omitted from a session-child operation, and
  the create and batch-settle bodies set ``extra="forbid"`` so an agent build
  that invents a field fails loudly instead of having it silently dropped.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from shared.session_subagent_authority import (
    SessionParentAuthority as AgentSessionSubagentAuthority,
)
from shared.session_subagent_batch import (
    BATCH_MAX_MEMBERS,
    BATCH_MESSAGE_MAX_CHARS,
    MEMBER_MESSAGE_MAX_CHARS,
)
from shared.subagent_parent_authority import ParentExecutionAuthority


class AgentThreadCreateRequest(BaseModel):
    """Request from agent to create its own thread on startup."""

    config_name: str = Field("session_base", description="Agent config name")
    permission_mode: str = Field("supervised", description="Permission mode")
    title: str = Field("Local Session", description="Session title")


class AgentThreadMessageRequest(BaseModel):
    """Request from agent to save a message."""

    role: str
    content: str | None = None
    tool_calls: list[dict] | None = None
    turn_number: int | None = None
    metrics: dict | None = None
    # Links a role='tool' row back to its originating tool_calls[].id.
    tool_call_id: str | None = None
    # Reasoning content captured from role='ai' rows. See migration 0011.
    thinking: str | None = None
    # Component columns added in migration 0019 — all optional/nullable.
    reasoning: Any | None = None
    tool_results: Any | None = None
    provider: str | None = None
    provider_raw: Any | None = None
    additional_kwargs: dict | None = None
    response_metadata: dict | None = None


class AgentSubagentThreadCreateRequest(BaseModel):
    """Body of ``POST /api/agents/jobs/{job_id}/subagents`` — what the agent-side
    subagent ledger knows at spawn (``SubagentLedger.open``)."""

    parent_authority: ParentExecutionAuthority
    handle: str = Field(
        ..., min_length=1, max_length=120, description="<type>-<4 hex> handle"
    )
    subagent_type: str = Field(
        ..., min_length=1, max_length=120, description="Roster entry name"
    )
    # The agent's in-process subagent id becomes the row id, so the audit
    # rows, the llm_requests rows and the thread share one identity.
    subagent_id: UUID | None = None
    parent_tool_call_id: str | None = Field(default=None, max_length=512)
    # Kept as an explicit NULL-only compatibility field while rolling agents
    # stop sending it. A worker child belongs solely to the job in the path;
    # session children use the separate /api/agents/threads/... routes below.
    parent_thread_id: None = None
    isolation: str = Field(default="shared", max_length=32)
    write_policy: str = Field(default="none", max_length=32)
    owned_paths: list[str] = Field(default_factory=list, max_length=128)
    brief_description: str = Field(default="", max_length=2000)
    parent_iteration: int | None = None
    fork: bool = False
    run_in_background: bool = False
    initial_status: Literal["queued", "running"] = "running"


class AgentSubagentThreadReopenRequest(BaseModel):
    """Exact ended generation to rotate before reviving a child."""

    runtime_generation: UUID
    parent_authority: ParentExecutionAuthority


class AgentSubagentThreadQueryRequest(BaseModel):
    """Exact parent authority for an internal generation-bearing read."""

    parent_authority: ParentExecutionAuthority


class AgentSubagentThreadTerminalRequest(BaseModel):
    """One exact child generation plus its stable worker delivery intent."""

    runtime_generation: UUID
    parent_authority: ParentExecutionAuthority
    delivery_id: UUID
    message: str = Field(..., min_length=1, max_length=200_000)
    timestamp: datetime
    subagent_status: str = Field(..., min_length=1, max_length=120)
    outcome: str | None = Field(default=None, max_length=4000)
    turns: int | None = Field(default=None, ge=0)
    tokens: int | None = Field(default=None, ge=0)
    report_path: str | None = Field(default=None, max_length=4000)
    error: str | None = Field(default=None, max_length=20_000)


class AgentSessionSubagentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parent_authority: AgentSessionSubagentAuthority
    handle: str = Field(..., min_length=1, max_length=120)
    subagent_type: str = Field(..., min_length=1, max_length=120)
    subagent_id: UUID | None = None
    parent_tool_call_id: str | None = Field(default=None, max_length=512)
    parent_input_message_id: UUID | None = None
    parent_ai_message_id: UUID | None = None
    isolation: str = Field(default="shared", max_length=32)
    write_policy: str = Field(default="none", max_length=32)
    owned_paths: list[str] = Field(default_factory=list, max_length=128)
    brief_description: str = Field(default="", max_length=2000)
    parent_iteration: int | None = None
    fork: bool = False
    run_in_background: bool = False
    initial_status: Literal["queued", "running"] = "running"


class AgentSessionSubagentQueryRequest(BaseModel):
    parent_authority: AgentSessionSubagentAuthority


class AgentSessionSubagentByCallRequest(AgentSessionSubagentQueryRequest):
    parent_tool_call_id: str = Field(..., min_length=1, max_length=512)


class AgentSessionSubagentReopenRequest(AgentSessionSubagentQueryRequest):
    runtime_generation: UUID


class AgentSessionSubagentTerminalRequest(AgentSessionSubagentQueryRequest):
    runtime_generation: UUID
    subagent_status: str = Field(..., min_length=1, max_length=120)
    delivery_id: UUID | None = None
    message: str | None = Field(default=None, max_length=200_000)
    outcome: str | None = Field(default=None, max_length=4000)
    turns: int | None = Field(default=None, ge=0)
    tokens: int | None = Field(default=None, ge=0)
    report_path: str | None = Field(default=None, max_length=4000)
    error: str | None = Field(default=None, max_length=20_000)
    foreground_orphan_recovery: bool = False


class AgentSessionSubagentBatchMember(BaseModel):
    """One child the successor names in a batch settle, at its generation.

    The terminal facts use the single-child terminal request's bounds. A
    child that ended before the restart states its stored facts; a child that
    was still live states ``interrupted`` / ``interrupted:parent_restart``.
    ``message`` is the text of the call's result, required exactly for the
    calls the recovery plan marks ``needs_message``.
    """

    model_config = ConfigDict(extra="forbid")

    thread_id: UUID
    runtime_generation: UUID
    subagent_status: str = Field(..., min_length=1, max_length=120)
    outcome: str | None = Field(default=None, max_length=4000)
    turns: int | None = Field(default=None, ge=0)
    tokens: int | None = Field(default=None, ge=0)
    report_path: str | None = Field(default=None, max_length=4000)
    error: str | None = Field(default=None, max_length=20_000)
    message: str | None = Field(default=None, max_length=MEMBER_MESSAGE_MAX_CHARS)


class AgentSessionSubagentBatchSettleRequest(AgentSessionSubagentQueryRequest):
    """Settle one abandoned delegation turn: the input and its owed children.

    Bounded per member and in total: every text lands in one transaction and
    in one parent transcript.
    """

    model_config = ConfigDict(extra="forbid")

    parent_input_message_id: UUID
    parent_iteration: int = Field(..., ge=1, strict=True)
    members: list[AgentSessionSubagentBatchMember] = Field(
        default_factory=list, max_length=BATCH_MAX_MEMBERS
    )

    @model_validator(mode="after")
    def _bounded_batch(self) -> "AgentSessionSubagentBatchSettleRequest":
        children = [member.thread_id for member in self.members]
        if len(set(children)) != len(children):
            raise ValueError("a batch settle names one child twice")
        total = sum(len(member.message or "") for member in self.members)
        if total > BATCH_MESSAGE_MAX_CHARS:
            raise ValueError(
                f"batch settle messages exceed {BATCH_MESSAGE_MAX_CHARS} characters"
            )
        return self


__all__ = [
    "AgentSessionSubagentBatchMember",
    "AgentSessionSubagentBatchSettleRequest",
    "AgentSessionSubagentByCallRequest",
    "AgentSessionSubagentCreateRequest",
    "AgentSessionSubagentQueryRequest",
    "AgentSessionSubagentReopenRequest",
    "AgentSessionSubagentTerminalRequest",
    "AgentSubagentThreadCreateRequest",
    "AgentSubagentThreadQueryRequest",
    "AgentSubagentThreadReopenRequest",
    "AgentSubagentThreadTerminalRequest",
    "AgentThreadCreateRequest",
    "AgentThreadMessageRequest",
]
