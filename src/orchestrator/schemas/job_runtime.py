"""Job start and completion wire contracts."""

from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field
from shared.pinned_session_identity import PinnedJobRecipient


class JobStartRequest(BaseModel):
    """Request sent to agent to start a job."""

    # This is an internal wire contract.  Silently ignoring an undeclared
    # constructor kwarg can make the producer appear to populate a required
    # field while model_dump() drops it before delivery.
    model_config = ConfigDict(extra="forbid")

    job_id: str
    pinned_delivery_id: UUID | None = None
    pinned_projection_digest: str | None = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$",
    )
    pinned_delivery_proof: str | None = Field(
        default=None, repr=False, pattern=r"^[0-9a-f]{64}$",
    )
    description: str
    upload_id: str | None = None
    config_upload_id: str | None = None
    instructions_upload_id: str | None = None
    document_path: str | None = None
    document_dir: str | None = None
    config_name: str = "worker_base"
    config_override: dict[str, Any] | None = None
    resolved_config: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Orchestrator-resolved config blob (serialize_resolved_config shape). "
            "Delivered instead of config_override when EXPERTS_DB_ENABLED; the "
            "agent hydrates it. The orchestrator owns resolution and the freeze."
        ),
    )
    context: dict[str, Any] | None = None
    instructions: str | None = None
    git_remote_url: str | None = None
    datasources: list[dict[str, Any]] | None = None
    repositories: list[dict[str, Any]] | None = Field(
        default=None,
        description="Project repositories for workspace setup",
    )
    managed_repository_credentials: list[dict[str, Any]] | None = Field(
        default=None,
        repr=False,
        description="Hidden server-owned repository authority transport",
    )
    branch_name: str | None = Field(
        default=None,
        description="Git branch name for this job's workspace",
    )
    project_id: str | None = Field(
        default=None,
        description="Project ID for connector resolution",
    )
    runtime_actor: dict[str, Any] | None = Field(
        default=None,
        description="Hidden server-derived runtime actor context",
    )
    workspace_runtime: dict[str, Any] | None = Field(
        default=None,
        description="Safe server-owned workspace runtime authority projection",
    )
    workspace_provisioner: str | None = Field(
        default=None,
        description="Server-derived physical workspace provisioner",
    )
    delegation_context: str | None = Field(
        default=None,
        description="Shared context from parent delegation",
    )
    workspace_generation: str | None = Field(
        default=None,
        description="Control-plane-attested Kubernetes backing UID",
    )
    workspace_runtime_incarnation: str | None = Field(
        default=None,
        description="Control-plane-attested current workspace Pod UID",
    )
    workspace_ssh_host_key_fingerprint: str | None = Field(
        default=None,
        description="Control-plane-attested SSH host-key fingerprint",
    )
    workspace_owner_kind: Literal["job", "session"] | None = Field(
        default=None,
        description="Kind used by the workspace entrypoint process tag",
    )
    workspace_owner_id: str | None = Field(
        default=None,
        description="Owner UUID used by the workspace entrypoint process tag",
    )
    recipient: PinnedJobRecipient | None = Field(
        default=None,
        repr=False,
        description="Hidden server-owned pinned runtime recipient authority",
    )


class JobCompleteRequest(BaseModel):
    """Result payload sent by the agent after a job finishes processing."""

    should_stop: bool = Field(False, description="Whether the graph stopped")
    goal_achieved: bool = Field(False, description="Whether the goal was achieved")
    error: dict[str, Any] | None = Field(None, description="Error dict if job failed")
    freeze_data: dict[str, Any] | None = Field(
        None, description="Freeze data from the graph state"
    )
    lease_token: int | None = Field(
        None,
        ge=1,
        description=(
            "Current worker_batch fencing token. Required only for stateless jobs."
        ),
    )
    agent_id: UUID | None = Field(
        None,
        description=(
            "Registered agent UUID. Required only for pinned jobs when the "
            "durable completion-command gate is enabled."
        ),
    )
    pinned_delivery_id: UUID | None = None
    pinned_projection_digest: str | None = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$",
    )
    pinned_delivery_proof: str | None = Field(
        default=None, repr=False, pattern=r"^[0-9a-f]{64}$",
    )
    pinned_process_generation: str | None = Field(default=None, max_length=128)
    pinned_pod_uid: str | None = Field(default=None, max_length=128)
    client_report_id: UUID | None = Field(
        None,
        description=(
            "Per-stop UUID idempotency key. Optional during rolling upgrades; "
            "new agents persist and resend it with the exact completion body."
        ),
    )
