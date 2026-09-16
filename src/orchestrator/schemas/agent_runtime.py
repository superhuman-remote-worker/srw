"""Agent registration and heartbeat wire contracts."""

from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, Field, field_validator
from shared.runtime.core.product_capabilities import (
    ComponentProvenance,
    ProvenanceStatus,
)


class WorkspaceRecoveryReport(BaseModel):
    """Worker observations only: no stop proof or successor authority."""

    model_config = {"extra": "forbid"}

    lease_token: int = Field(gt=0, strict=True)
    pod_name: str = Field(min_length=1, max_length=253, pattern=r"^\S+$")
    pod_uid: UUID
    request_id: UUID
    code: Literal[
        "workspace_runtime_not_ready",
        "workspace_transport_unavailable",
        "workspace_replacement_observed",
        "workspace_identity_conflict",
        "tool_outcome_unknown",
        "checkpoint_unavailable",
    ]


class AgentRegistration(BaseModel):
    """Request body for agent registration."""

    config_name: str = Field(..., description="Agent configuration name")
    pod_ip: str = Field(..., description="Agent IP address for receiving commands")
    hostname: str | None = Field(None, description="Pod/host name")
    pod_port: int = Field(8001, description="Agent API port")
    pid: int | None = Field(None, description="Process ID")
    agent_mode: str = Field(
        "worker", description="Agent mode: 'worker' or 'persistent'"
    )
    thread_id: str | None = Field(None, description="Thread UUID for persistent agents")
    session_runtime_generation: UUID | None = Field(
        None,
        description=(
            "Exact pinned runtime generation injected into a dedicated pod. "
            "Protected runtimes must present it at registration."
        ),
    )
    build_sha: str | None = Field(
        None, description="Build commit SHA baked into the agent image"
    )
    product_provenance: ComponentProvenance = Field(
        default_factory=lambda: ComponentProvenance(
            provenance_status=ProvenanceStatus.UNAVAILABLE
        ),
        description="Bounded declared provenance for the registering agent image",
    )
    pod_uid: str | None = Field(
        None,
        description=(
            "K8s-assigned metadata.uid of the agent pod, self-reported via "
            "the Kubernetes downward API. Used by the session router to "
            "stamp ownerReferences on per-session Service/Ingress resources."
        ),
    )

    @field_validator("product_provenance")
    @classmethod
    def reject_self_verified_provenance(
        cls,
        value: ComponentProvenance,
    ) -> ComponentProvenance:
        if value.provenance_status is ProvenanceStatus.VERIFIED:
            raise ValueError(
                "agent registration may not self-assert verified provenance"
            )
        return value


class AgentRegistrationResponse(BaseModel):
    """Response from agent registration."""

    agent_id: str
    heartbeat_interval_seconds: int
    dispatch_process_generation: str = Field(
        ...,
        description=(
            "Server-minted exact process generation required on every pinned "
            "job mutation"
        ),
    )
    pinned_runtime_generation_contract: int = 1
    session_runtime_generation: str | None = None
    session_runtime_attach_token: str | None = Field(
        None,
        description=(
            "Per-process pinned-runtime authority minted by the final "
            "registration bind and echoed on maintenance requests"
        ),
    )
    runtime_actor: dict[str, Any] | None = Field(
        None,
        description=(
            "Hidden server-derived actor context returned only after a "
            "thread-bound pod proves its provision-time bootstrap credential"
        ),
    )


class AgentHeartbeat(BaseModel):
    """Request body for agent heartbeat."""

    status: str = Field(
        ...,
        description="Agent status",
        # The `agents.valid_agent_status` check constraint is the authority
        # here, and it does not accept 'available': 0001_initial creates the
        # constraint with that value and then immediately drops and re-adds it
        # without, adding 'session'. This pattern kept advertising the dropped
        # value, so a heartbeat reporting it passed validation and then took
        # the DB error as a 500 — which returned the failing row's contents to
        # the caller. Nothing in the agent ever sent it. 'offline' stays out
        # deliberately: it is what the orchestrator writes about an agent that
        # stopped reporting, not something an agent may claim about itself.
        pattern="^(booting|ready|working|session|draining|completed|failed)$",
    )
    current_job_id: str | None = Field(None, description="Current job UUID if working")
    metrics: dict[str, Any] | None = Field(
        None,
        description="Optional metrics (memory_mb, cpu_percent, tokens_processed)",
    )
    graph_progress: int | None = Field(
        None,
        description="Monotonic graph-progress marker from worker heartbeat path",
    )
    session_runtime_generation: UUID | None = Field(
        None,
        description="Exact generation of a bound pinned session runtime",
    )
    session_runtime_attach_token: UUID | None = Field(
        None,
        description="Exact warm-attach attempt for a bound pinned runtime",
    )
