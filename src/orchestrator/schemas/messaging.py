"""Request bodies for the agent-messaging and officer-route surfaces.

Moved verbatim from ``orchestrator.main`` (R1.B07 lane M, census group
``J_messaging``). Field names, defaults, bounds and descriptions are the
published OpenAPI request schema, so they are part of each route's identity
and none of them is rewritten here.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


class MessageSendRequest(BaseModel):
    """Request body for agent-initiated message send."""

    to: str = Field(
        ...,
        description="Recipient: 'user' for job owner, or display name / email of a project member",
    )
    subject: str = Field(..., max_length=200, description="Subject line")
    message: str = Field(..., max_length=5000, description="Message body (markdown)")
    mode: str = Field("async", description="'async' or 'blocking'")
    thread_id: str | None = Field(
        None, description="Existing thread ID, or null for new thread"
    )
    project_id: str | None = Field(
        None, description="Project ID for member resolution (auto-filled from job)"
    )
    lease_token: int | None = Field(
        None,
        ge=1,
        description="Exact stateless worker lease; ignored for pinned jobs",
    )
    agent_id: str | None = Field(
        None,
        description="Exact pinned agent assignment; ignored for stateless jobs",
    )
    pinned_delivery_id: UUID | None = Field(
        None, description="Exact accepted pinned Job delivery intent",
    )
    pinned_projection_digest: str | None = Field(
        None, pattern=r"^sha256:[0-9a-f]{64}$",
    )
    pinned_delivery_proof: str | None = Field(
        None, repr=False, pattern=r"^[0-9a-f]{64}$",
    )
    pinned_process_generation: str | None = Field(None, max_length=128)
    pinned_pod_uid: str | None = Field(None, max_length=128)
    purpose: str | None = Field(
        None,
        description=(
            "Optional self-declared label (question|blocker|update). "
            "Presentation/coalescing only — routing never trusts it; "
            "mode='blocking' stays the mechanical signal."
        ),
    )
    routing_generation: UUID | None = Field(
        None,
        description=(
            "Internal idempotency identity for this logical send. It conveys no "
            "audience, quota, recipient, or routing authority."
        ),
    )


class MessageReplyRequest(BaseModel):
    """Request body for human reply to agent message."""

    message: str = Field(..., description="Reply body")
    urgent: bool = Field(False, description="Deliver as immediate interrupt")


class OfficerMessageReplyRequest(BaseModel):
    """Body for the officer's reply on a worker message thread."""

    message: str = Field(..., max_length=5000, description="Answer for the worker")


class OfficerMessageEscalateRequest(BaseModel):
    """Body for escalating a worker message thread to the user."""

    context: str | None = Field(
        None,
        max_length=5000,
        description="Officer context delivered (clearly delimited) with the "
        "original worker message",
    )


class OfficerMessageAckRequest(BaseModel):
    """Body for acknowledging (closing) an async worker message route."""

    note: str | None = Field(None, max_length=1000, description="Optional note")


class GuidanceAckRequest(BaseModel):
    """Body for the agent's guidance-delivery ack (P1-A)."""

    guidance_ids: list[str] = Field(
        default_factory=list,
        description="context.pending_guidance entry ids rendered into the "
        "worker's LLM context",
    )
    reply_threads: list[str] = Field(
        default_factory=list,
        description="thread ids whose context.queued_replies were drained "
        "at a phase boundary",
    )
    reply_keys: list[str] = Field(
        default_factory=list,
        description="exact durable identities of queued replies absorbed by "
        "a stateless-worker checkpoint",
    )
    feedback_keys: list[str] = Field(
        default_factory=list,
        description="exact generations of queued feedback absorbed by a "
        "stateless-worker checkpoint",
    )
    delegation_keys: list[str] = Field(
        default_factory=list,
        description="exact generations of delegation results absorbed by a "
        "stateless-worker checkpoint",
    )
    checkpoint_id: str | None = Field(
        default=None,
        description="durable LangGraph checkpoint proving stateless delivery",
    )


__all__ = [
    "GuidanceAckRequest",
    "MessageReplyRequest",
    "MessageSendRequest",
    "OfficerMessageAckRequest",
    "OfficerMessageEscalateRequest",
    "OfficerMessageReplyRequest",
]
