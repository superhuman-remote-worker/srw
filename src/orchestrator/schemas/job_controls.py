"""Request contracts for job resume, approval, VM, and sudo controls."""

from uuid import UUID

from pydantic import BaseModel, Field


class SudoApproveRequest(BaseModel):
    """Request body for approving a sudo request."""

    reason: str = Field("", description="Optional approval reason")


class SudoDenyRequest(BaseModel):
    """Request body for denying a sudo request."""

    reason: str = Field(..., description="Denial reason (required)")


class SudoRuleCreateRequest(BaseModel):
    """Request body for creating an auto-approval rule."""

    pattern: str = Field(..., description="fnmatch pattern (e.g. 'apt-get install *')")
    action: str = Field(..., description="'approve', 'deny', or 'review'")
    priority: int = Field(100, ge=0, le=1000, description="Lower = higher priority")
    description: str = Field("", description="Human-readable description")


class JobUpdateRequest(BaseModel):
    """Request body for changing a job's mutable metadata."""

    title: str | None = Field(
        ...,
        description=(
            "Display name for the job, at most 200 characters. Null or blank "
            "clears it, and the job is shown by its description again. The "
            "task text (description) does not change."
        ),
    )


class JobResumeRequest(BaseModel):
    """Request body for resuming a failed or paused job."""

    feedback: str | None = Field(
        None, description="Optional feedback to inject before resuming"
    )
    agent_id: str | None = Field(
        None, description="Override agent ID if original is offline"
    )


class JobApproveRequest(BaseModel):
    """Request body for approving a frozen job."""

    notes: str | None = Field(None, description="Optional reviewer notes")


class WorkspaceRecoveryRetryRequest(BaseModel):
    """Idempotent explicit retry of one paused workspace recovery."""

    operation_id: UUID
    request_id: UUID


__all__ = [
    "JobApproveRequest",
    "JobResumeRequest",
    "SudoApproveRequest",
    "SudoDenyRequest",
    "SudoRuleCreateRequest",
    "WorkspaceRecoveryRetryRequest",
]
