"""Request and response models for Universal Agent API.

Pydantic models for the FastAPI application, providing type-safe
request validation and response serialization.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

# The pinned-worker recipient envelope lives in ``src.shared`` so the
# orchestrator can import it without pulling in this package's
# ``__init__`` -> ``app`` -> ``agent`` chain (and its agent-only
# dependencies). Re-exported here for the agent-side API surface.
from shared.pinned_session_identity import (  # noqa: F401
    PinnedJobRecipient,
    pinned_job_recipient_matches,
)


class JobStatus(str, Enum):
    """Job processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class HealthStatus(str, Enum):
    """Service health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


# Request Models


class JobSubmitRequest(BaseModel):
    """Request to submit a new job for processing."""

    document_path: Optional[str] = Field(
        default=None,
        description="Path to document to process (for Creator agent)",
    )
    description: Optional[str] = Field(
        default=None,
        description="Job description - what the agent should accomplish",
    )
    requirement_id: Optional[str] = Field(
        default=None,
        description="Requirement ID to validate (for Validator agent)",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional job metadata",
    )
    priority: str = Field(
        default="medium",
        description="Job priority (high, medium, low)",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "document_path": "/data/documents/gobd_spec.pdf",
                "description": "Extract GoBD compliance requirements",
                "priority": "high",
            }
        }
    )


class JobCancelRequest(BaseModel):
    """Request to cancel a running job."""

    reason: Optional[str] = Field(
        default=None,
        description="Reason for cancellation",
    )
    cleanup: bool = Field(
        default=True,
        description="Clean up workspace and resources",
    )


# Response Models


class JobSubmitResponse(BaseModel):
    """Response after submitting a job."""

    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Initial job status")
    created_at: datetime = Field(..., description="Job creation timestamp")
    message: str = Field(..., description="Status message")


class JobStatusResponse(BaseModel):
    """Response for job status query."""

    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="Current status")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    iteration: int = Field(default=0, description="Current iteration count")
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if failed")
    result: Optional[Dict[str, Any]] = Field(
        None, description="Result data if complete"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job_id": "abc123-def456",
                "status": "processing",
                "created_at": "2024-01-08T10:30:00Z",
                "iteration": 42,
            }
        }
    )


class JobListResponse(BaseModel):
    """Response for listing jobs."""

    jobs: List[JobStatusResponse] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total job count")
    page: int = Field(default=1, description="Current page")
    page_size: int = Field(default=20, description="Page size")


class HealthResponse(BaseModel):
    """Health check response."""

    status: HealthStatus = Field(..., description="Overall health status")
    agent_id: str = Field(..., description="Agent identifier")
    agent_name: str = Field(..., description="Agent display name")
    uptime_seconds: float = Field(..., description="Time since start")
    checks: Dict[str, bool] = Field(..., description="Individual health checks")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "healthy",
                "agent_id": "creator",
                "agent_name": "Creator Agent",
                "uptime_seconds": 3600.5,
                "checks": {
                    "database": True,
                    "llm": True,
                    "workspace": True,
                },
            }
        }
    )


class ReadyResponse(BaseModel):
    """Readiness probe response."""

    ready: bool = Field(..., description="Whether agent is ready to accept jobs")
    message: str = Field(..., description="Status message")
    connections: Dict[str, bool] = Field(
        ..., description="Connection status for dependencies"
    )
    capabilities: Dict[str, StrictBool | StrictInt] = Field(
        default_factory=dict,
        description="Server-owned runtime protocol capabilities",
    )
    session_identity_fingerprint: Optional[str] = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
        description="Non-secret fingerprint of the exact pinned runtime binding",
    )


class AgentStatusResponse(BaseModel):
    """Detailed agent status response."""

    agent_id: str = Field(..., description="Agent identifier")
    display_name: str = Field(..., description="Display name")
    initialized: bool = Field(..., description="Whether agent is initialized")
    current_job: Optional[str] = Field(None, description="Currently processing job")
    jobs_processed: int = Field(..., description="Total jobs processed")
    uptime_seconds: float = Field(..., description="Uptime in seconds")
    connections: Dict[str, bool] = Field(..., description="Connection status")
    config: Dict[str, Any] = Field(..., description="Configuration summary")
    research_providers: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Secret-free cached health for optional research providers",
    )


class ErrorResponse(BaseModel):
    """Error response model."""

    error: str = Field(..., description="Error type/code")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    job_id: Optional[str] = Field(None, description="Related job ID if applicable")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "error": "job_not_found",
                "message": "Job with ID 'abc123' not found",
                "job_id": "abc123",
            }
        }
    )


class WorkspaceFileResponse(BaseModel):
    """Response for workspace file operations."""

    path: str = Field(..., description="File path within workspace")
    content: Optional[str] = Field(None, description="File content (if requested)")
    size: int = Field(..., description="File size in bytes")
    modified_at: datetime = Field(..., description="Last modification time")
    is_directory: bool = Field(default=False, description="Whether path is a directory")


class WorkspaceListResponse(BaseModel):
    """Response for listing workspace contents."""

    job_id: str = Field(..., description="Job identifier")
    path: str = Field(..., description="Directory path")
    files: List[WorkspaceFileResponse] = Field(..., description="Directory contents")


class TodoResponse(BaseModel):
    """Response for todo list query."""

    job_id: str = Field(..., description="Job identifier")
    todos: List[Dict[str, Any]] = Field(..., description="Todo items")
    progress: Dict[str, int] = Field(..., description="Progress statistics")


class MetricsResponse(BaseModel):
    """Response for metrics endpoint."""

    agent_id: str = Field(..., description="Agent identifier")
    timestamp: datetime = Field(..., description="Metrics timestamp")
    jobs_total: int = Field(..., description="Total jobs processed")
    jobs_success: int = Field(..., description="Successful jobs")
    jobs_failed: int = Field(..., description="Failed jobs")
    average_duration_seconds: Optional[float] = Field(
        None, description="Average job duration"
    )
    current_iterations: int = Field(default=0, description="Iterations in current job")
    uptime_seconds: float = Field(..., description="Agent uptime")


# =============================================================================
# Orchestrator Integration Models
# =============================================================================


class PinnedSessionRecipient(BaseModel):
    """Server-owned identity envelope for one pinned-session mutation."""

    model_config = ConfigDict(extra="forbid")

    expected_thread_id: str = Field(min_length=1)
    expected_agent_id: str = Field(min_length=1)
    expected_pod_uid: Optional[str] = None
    expected_process_generation: str = Field(min_length=1)


def pinned_session_recipient_matches(
    recipient: Optional[PinnedSessionRecipient],
    *,
    thread_id: Optional[str],
    agent_id: Optional[str],
    pod_uid: Optional[str],
    process_generation: Optional[str],
) -> bool:
    """Return whether an internal session mutation targets this process."""

    if recipient is None:
        return False
    return bool(
        str(thread_id or "") == recipient.expected_thread_id
        and str(agent_id or "") == recipient.expected_agent_id
        and (str(pod_uid or "").strip() or None)
        == (str(recipient.expected_pod_uid or "").strip() or None)
        and str(process_generation or "") == recipient.expected_process_generation
    )


class JobStartRequest(BaseModel):
    """Request from orchestrator to start a job."""

    job_id: str = Field(..., description="Job UUID assigned by orchestrator")
    pinned_delivery_id: Optional[UUID] = Field(
        default=None, description="Exact server-frozen pinned Job delivery intent",
    )
    pinned_projection_digest: Optional[str] = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$",
        description="Digest of the accepted pinned Job wire projection",
    )
    pinned_delivery_proof: Optional[str] = Field(
        default=None, repr=False, pattern=r"^[0-9a-f]{64}$",
        description="Hidden proof to echo only in authenticated agent reports",
    )
    description: str = Field(
        ..., description="Job description - what the agent should accomplish"
    )
    upload_id: Optional[str] = Field(
        default=None,
        description="Upload ID for document files (from /api/uploads)",
    )
    config_upload_id: Optional[str] = Field(
        default=None,
        description="Upload ID for config YAML override",
    )
    instructions_upload_id: Optional[str] = Field(
        default=None,
        description="Upload ID for instructions markdown file",
    )
    document_path: Optional[str] = Field(
        default=None,
        description="Path to document to process",
    )
    document_dir: Optional[str] = Field(
        default=None,
        description="Directory containing documents",
    )
    expert_id: Optional[str] = Field(
        default=None,
        description="DB-backed expert UUID for this job",
    )
    config_name: str = Field(
        default="worker_base",
        description="Agent configuration name",
    )
    config_override: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Per-job configuration overrides",
    )
    resolved_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Orchestrator-resolved config blob (serialize_resolved_config shape). "
            "When present the agent hydrates it directly instead of resolving "
            "config_name + config_override locally — the orchestrator owns "
            "resolution and the freeze. Absent → today's path (fallback)."
        ),
    )
    context: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional context dictionary",
    )
    instructions: Optional[str] = Field(
        default=None,
        description="Additional inline instructions for the agent",
    )
    git_remote_url: Optional[str] = Field(
        default=None,
        description="Git remote URL for workspace delivery (set by orchestrator)",
    )
    datasources: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Resolved connector details (set by orchestrator)",
    )
    repositories: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Project repositories (jobs, source, reference)",
    )
    managed_repository_credentials: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        repr=False,
        description="Hidden server-owned repository authority transport",
    )
    branch_name: Optional[str] = Field(
        default=None,
        description="Git branch for job workspace",
    )
    project_id: Optional[str] = Field(
        default=None,
        description="Project ID for connector scoping",
    )
    runtime_actor: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Hidden server-derived runtime actor context",
    )
    workspace_runtime: Optional[Dict[str, Any]] = Field(
        default=None,
        description=("Safe server-owned requested/assigned/effective tier observation"),
    )
    workspace_provisioner: Optional[str] = Field(
        default=None,
        description="Hidden server-derived physical workspace provisioner",
    )
    workspace_generation: Optional[str] = Field(
        default=None,
        description="Control-plane-attested Kubernetes backing UID",
    )
    workspace_runtime_incarnation: Optional[str] = Field(
        default=None,
        description="Control-plane-attested current workspace Pod UID",
    )
    workspace_ssh_host_key_fingerprint: Optional[str] = Field(
        default=None,
        description="Control-plane-attested SSH host-key fingerprint",
    )
    workspace_owner_kind: Optional[Literal["job", "session"]] = Field(
        default=None,
        description="Kind used by the workspace entrypoint process tag",
    )
    workspace_owner_id: Optional[str] = Field(
        default=None,
        description="Owner UUID used by the workspace entrypoint process tag",
    )
    recipient: Optional[PinnedJobRecipient] = Field(
        default=None,
        repr=False,
        description="Hidden server-owned pinned runtime recipient authority",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "job_id": "abc123-def456-789",
                "description": "Extract requirements from document",
                "document_path": "/data/documents/spec.pdf",
            }
        }
    )


class JobStartResponse(BaseModel):
    """Response after accepting a job from orchestrator."""

    job_id: str = Field(..., description="Accepted job ID")
    pinned_delivery_id: Optional[UUID] = None
    pinned_projection_digest: Optional[str] = None
    status: str = Field(default="accepted", description="Acceptance status")
    message: str = Field(
        default="Job processing started",
        description="Status message",
    )


class JobCancelByOrchestratorRequest(BaseModel):
    """Request from orchestrator to cancel current job."""

    reason: Optional[str] = Field(
        default=None,
        description="Reason for cancellation",
    )
    recipient: Optional[PinnedJobRecipient] = Field(
        default=None,
        repr=False,
        description="Hidden server-owned pinned runtime recipient authority",
    )


class JobResumeRequest(BaseModel):
    """Request to resume a job from last completed phase snapshot."""

    job_id: str = Field(..., description="Job ID to resume")
    pinned_delivery_id: Optional[UUID] = Field(
        default=None, description="Exact server-frozen pinned Job delivery intent",
    )
    pinned_projection_digest: Optional[str] = Field(
        default=None, pattern=r"^sha256:[0-9a-f]{64}$",
        description="Digest of the accepted pinned Job wire projection",
    )
    pinned_delivery_proof: Optional[str] = Field(
        default=None, repr=False, pattern=r"^[0-9a-f]{64}$",
        description="Hidden proof to echo only in authenticated agent reports",
    )
    config_name: Optional[str] = Field(
        default=None,
        description="Job's config name (for validation that agent has correct config)",
    )
    config_upload_id: Optional[str] = Field(
        default=None,
        description="Config upload ID to reload the original job configuration",
    )
    config_override: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Inline config overrides from the original job",
    )
    resolved_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Orchestrator-resolved config blob for the resumed job. When "
            "present the agent hydrates this complete, credential-injected "
            "snapshot instead of retaining its generic boot config. Absent "
            "keeps the legacy resume fallback."
        ),
    )
    feedback: Optional[str] = Field(
        default=None,
        description="Optional feedback to inject before resuming",
    )
    feedback_reason: Optional[str] = Field(
        default=None,
        description=(
            "Why the job was resumed with feedback (e.g. critic return, "
            "supervisor escalation, reviewer feedback). Rendered verbatim in "
            "the [FEEDBACK_RESUME] banner; omitted -> honest generic fallback."
        ),
    )
    delegation_results: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Completed delegated job results for the resumed parent",
    )
    datasources: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Resolved connector details (set by orchestrator)",
    )
    repositories: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Project repositories re-authorized by the orchestrator",
    )
    managed_repository_credentials: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        repr=False,
        description="Hidden server-owned repository authority transport",
    )
    project_id: Optional[str] = Field(
        default=None,
        description="Project ID for knowledge base and memory scoping (set by orchestrator)",
    )
    runtime_actor: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Hidden server-derived runtime actor context",
    )
    workspace_runtime: Optional[Dict[str, Any]] = Field(
        default=None,
        description=("Safe server-owned requested/assigned/effective tier observation"),
    )
    workspace_provisioner: Optional[str] = Field(
        default=None,
        description="Hidden server-derived physical workspace provisioner",
    )
    workspace_generation: Optional[str] = Field(
        default=None,
        description="Control-plane-attested Kubernetes backing UID",
    )
    workspace_runtime_incarnation: Optional[str] = Field(
        default=None,
        description="Control-plane-attested current workspace Pod UID",
    )
    workspace_ssh_host_key_fingerprint: Optional[str] = Field(
        default=None,
        description="Control-plane-attested SSH host-key fingerprint",
    )
    workspace_owner_kind: Optional[Literal["job", "session"]] = Field(
        default=None,
        description="Kind used by the workspace entrypoint process tag",
    )
    workspace_owner_id: Optional[str] = Field(
        default=None,
        description="Owner UUID used by the workspace entrypoint process tag",
    )
    previous_status: Optional[str] = Field(
        default=None,
        description="Job status before resume. Graceful stops (cancelled, paused, pending_review, waiting) "
        "skip snapshot recovery; crashes (processing, failed) use snapshot recovery.",
    )
    git_remote_url: Optional[str] = Field(
        default=None,
        description=(
            "Job repo remote for the pod-handoff clone fallback: a resume "
            "onto a fresh workspace with no snapshot clones the job's own "
            "Gitea repo instead of starting blank (set by orchestrator)."
        ),
    )
    recipient: Optional[PinnedJobRecipient] = Field(
        default=None,
        repr=False,
        description="Hidden server-owned pinned runtime recipient authority",
    )
