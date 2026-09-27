"""The agent's thread-status request body.

R1.B06, root lane. Declared beside the route that accepts it so the published
request schema keeps the exact class name it had in ``orchestrator.main``.

Every optional field here is an *identity*, not a convenience. After the 0185
drained cutover a pinned status write must carry the reciprocal owner
credential, and the validator below is the first fence: an ``agent_id`` without
an exact ``process_generation`` is refused before any row is read.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class AgentRetirementActuatorRequest(BaseModel):
    """Exact End handoff; carries no endpoint, disk authority or zero proof."""

    agent_id: UUID
    pod_uid: str = Field(min_length=1, max_length=256)
    process_generation: str = Field(min_length=1, max_length=256)
    session_runtime_generation: UUID
    session_runtime_attach_token: UUID
    session_runtime_retirement_token: UUID
    retirement_disposition: Literal["ended"]
    retirement_permanent: bool = Field(strict=True)
    workspace_generation: UUID
    workspace_runtime_incarnation: UUID

    model_config = {"extra": "forbid"}


class AgentThreadStatusRequest(BaseModel):
    status: str
    # Optional exact owner credential for pinned teardown. Stateless callers
    # omit it; pinned status writes require it after the 0185 drained cutover so
    # the transition is serialized with the reciprocal thread/agent binding.
    agent_id: UUID | None = None
    pod_uid: UUID | None = None
    process_generation: str | None = Field(default=None, max_length=256)
    session_runtime_generation: UUID | None = None
    session_runtime_attach_token: UUID | None = None
    session_runtime_retirement_token: UUID | None = None
    # Immutable disposition installed by the begin-only `ending` edge.  A
    # retry may reuse that retirement token only for this exact outcome.
    retirement_disposition: Literal["ended", "suspended"] | None = None
    # Immutable owner-delete intent. Agents may only echo ``true`` from an
    # already-authorized lifecycle response; an agent-originated Begin is
    # always a resumable soft retirement.
    retirement_permanent: bool = False
    # Append-only settlement proof.  The agent may set this only after its
    # strict local teardown has drained shell jobs, overlays/mounts and the
    # ordinary event writer.  Physical workspace IDs bind the proof to the
    # exact backing captured by Begin.
    local_runtime_quiesced: bool = False
    local_quiescence_protocol: (
        Literal[
            "workspace_process_zero_v1",
            "agent_runtime_zero_v1",
            "workspace_actuator_zero_v1",
        ]
        | None
    ) = None
    workspace_generation: UUID | None = None
    workspace_runtime_incarnation: UUID | None = None

    @model_validator(mode="after")
    def validate_exact_pinned_process(self) -> "AgentThreadStatusRequest":
        if self.agent_id is not None and not str(self.process_generation or "").strip():
            raise ValueError("agent_id requires exact process_generation")
        return self
