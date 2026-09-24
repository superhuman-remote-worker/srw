"""Public documentation for GET /api/jobs, not a runtime response filter.

The route keeps its dict[str, Any] response serializer. These models are used
only in FastAPI's additional response documentation and in contract tests.
Changing them must not drop existing extension fields, coerce JSONB text, or
replace the route/store's authorization and private-field redaction.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class WorkspaceRecoveryView(BaseModel):
    """Coordinate-free public recovery contract shared by list/detail/progress."""

    operation_id: UUID
    state: str
    reason_code: str
    message: str
    started_at: datetime
    deadline_at: datetime
    next_check_at: datetime | None
    retryable: bool
    cleanup_pending: bool


class VMCreationView(BaseModel):
    """Bounded provisioning progress; Resume advice is not execution authority."""

    request_id: UUID
    state: str
    stage: str
    reason_code: str
    message: str
    resumable: bool


class WorkspaceLifecycleView(BaseModel):
    """Safe VM compute state, separate from the human wait or Job status."""

    state: str
    idle_expires_at: datetime | None = None
    reason_code: str | None = None
    next_retry_at: datetime | None = None


class PublicJobListItem(BaseModel):
    """Current list-row projection; additional public fields remain compatible."""

    model_config = ConfigDict(extra="allow")

    id: UUID
    description: str
    status: str
    completion_outcome_kind: str | None
    origin: str
    config_name: str | None
    assigned_agent_id: UUID | None
    user_id: UUID | None
    project_id: UUID | None
    parent_job_id: UUID | None
    priority: int
    repo_name: str | None
    branch_name: str | None
    merge_status: str | None
    diff_status: str | None
    exported_at: datetime | None
    exported_folder_handle: str | None
    error_message: str | None
    created_at: datetime
    created_by_thread_id: UUID | None
    snapshot_status: str | None
    project_name: str | None
    pending_approval: bool
    pending_approval_request_id: UUID | None
    display_root_id: UUID
    is_display_root: bool
    subjob_count: int = Field(
        description="Unfiltered descendant count, independent of visible child rows."
    )
    workspace_contract: dict[str, Any] = Field(
        description="Safe workspace tier/state projection; no private lease identity or endpoints."
    )
    workspace_recovery: WorkspaceRecoveryView | None = Field(
        description="Safe unresolved VM workspace recovery state; no controller coordinates or diagnostics."
    )
    vm_creation: VMCreationView | None = Field(
        description="Safe VM creation progress and advisory Resume availability."
    )
    workspace_lifecycle: WorkspaceLifecycleView | None = Field(
        description="Safe VM compute state; no runtime identities, coordinates or other users' leases."
    )
    audit_count: int | None = Field(
        description="Null when the optional audit service is unavailable; zero when available with no entries."
    )
    cloud_review_mode: str
    exported_folder_url: str | None


class PublicJobListFilters(BaseModel):
    """Applied filters, including defaults and deduplicated repeated values."""

    model_config = ConfigDict(extra="allow")

    status: list[str]
    origin: list[str] = Field(
        description="Empty means every origin; the API has no default origin filter."
    )
    project_id: list[str]
    has_project: bool | None
    include_archived_projects: bool
    search: str | None = Field(
        description="Preserves omitted (null) versus explicitly empty search text."
    )
    user_id: str | None = Field(
        description="Admin-only owner filter; a non-admin self-query is echoed as null."
    )


class PublicJobListPage(BaseModel):
    """A page of display roots with their matching children, not a row-count page."""

    model_config = ConfigDict(extra="allow")

    jobs: list[PublicJobListItem] = Field(
        description="Roots and matching children; the number of rows can exceed limit."
    )
    total: int | None = Field(
        description="Display-root count, capped at 10,000; null when include_total=false."
    )
    total_is_capped: bool = Field(
        description="True means total is a lower bound; false when counting was skipped."
    )
    has_more: bool = Field(
        description="Whether another display root exists, independent of total."
    )
    limit: int = Field(description="Requested number of display roots (1–500).")
    offset: int = Field(description="Number of display roots skipped.")
    as_of: str = Field(
        description=(
            "Creation-time watermark; pass back on later pages to exclude newer inserts. "
            "Not a snapshot of status changes or deletions. Generated UTC uses Z; accepted "
            "offset and naive input timestamps retain their existing serialization."
        )
    )
    filters: PublicJobListFilters


class JobListFailure(BaseModel):
    detail: str


class JobListValidationIssue(BaseModel):
    model_config = ConfigDict(extra="allow")

    loc: list[str | int]
    msg: str
    type: str


class JobListValidationFailure(BaseModel):
    detail: str | list[JobListValidationIssue]


# `responses`, rather than `response_model`, documents the existing payload
# without replacing the dictionary response field and its serializer.
JOB_LIST_RESPONSES = {
    200: {"model": PublicJobListPage},
    400: {
        "model": JobListFailure,
        "description": "Offset exceeds the supported window.",
    },
    401: {"model": JobListFailure, "description": "Authentication required."},
    403: {
        "model": JobListFailure,
        "description": "Approval, visibility or token scope refused.",
    },
    422: {
        "model": JobListValidationFailure,
        "description": "Invalid query or unsupported filter combination.",
    },
    500: {
        "model": JobListFailure,
        "description": "Existing storage/audit failure detail.",
    },
}
