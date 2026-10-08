"""Project, membership, repository and promotion request contracts."""

from typing import Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)


class ExternalKnowledgeBase(BaseModel):
    """An existing private GitHub repo to use as a project's live vault.

    Two ways to name that repo, and exactly one per request:

    * ``datasource_id`` — adopt a ``kb`` connector created earlier, which
      already holds the URL, branch and encrypted PAT. This is the cockpit's
      only path: a connector is created first, then attached here.
    * ``repo_url`` + ``token`` — the inline form, kept for MCP and other API
      callers that have no connector to point at.
    """

    datasource_id: str | None = Field(
        None,
        description="Existing OKF Knowledge Base connector to adopt as the vault",
    )
    repo_url: str | None = Field(None, description="Existing GitHub repository URL")
    branch: str = Field("main", description="Writable vault branch")
    token: SecretStr | None = Field(
        None, description="Fine-grained GitHub contents PAT"
    )
    forge: Literal["github"] | None = Field(
        None,
        description="Required for GitHub Enterprise; github.com is inferred",
    )

    @field_validator("branch")
    @classmethod
    def _valid_branch(cls, value: str) -> str:
        branch = str(value or "").strip()
        if (
            not branch
            or branch.startswith("-")
            or any(char in branch for char in ("\x00", "\n", "\r"))
        ):
            raise ValueError("branch must be a non-empty Git ref")
        return branch

    @field_validator("token")
    @classmethod
    def _valid_token(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and not value.get_secret_value().strip():
            raise ValueError("token must not be empty")
        return value

    @model_validator(mode="after")
    def _exactly_one_source(self) -> "ExternalKnowledgeBase":
        """One vault, named one way.

        A connector already carries branch, forge and credentials, so an inline
        field alongside it is ambiguous rather than additive — the request is
        rejected instead of silently preferring one side.
        """
        if self.datasource_id is not None:
            conflicting = sorted(
                {"repo_url", "token", "branch", "forge"} & self.model_fields_set
            )
            if conflicting:
                raise ValueError(
                    "datasource_id already carries the vault settings; remove "
                    + ", ".join(conflicting)
                )
        elif not (self.repo_url and self.token):
            raise ValueError("supply datasource_id, or both repo_url and token")
        return self


class ProjectCreate(BaseModel):
    """Request body for creating a project."""

    name: str = Field(..., description="Project name")
    description: str | None = Field(None, description="Project description")
    goal: str | None = Field(None, description="Project goal statement")
    default_config_name: str | None = Field(
        None, description="Default agent config for new jobs"
    )
    default_config_override: dict[str, Any] | None = Field(
        None, description="Default config overrides"
    )
    user_id: str = Field(..., description="Owner user UUID")
    external_kb: ExternalKnowledgeBase | None = Field(
        None,
        description="Existing private GitHub repo for the writable project KB",
    )


class ProjectUpdate(BaseModel):
    """Request body for updating a project."""

    name: str | None = None
    description: str | None = None
    goal: str | None = None
    # One vocabulary, validated here rather than only at the DB CHECK (§4.1 of
    # knowledge-base/knowledge/features/project_and_job_list_filtering.md).
    # `paused`/`completed` are still permitted by the constraint but nothing
    # has ever written them, and the cockpit's `deleted` was always rejected
    # by it — a 422 naming the field beats a 500 out of asyncpg. Tightening
    # the constraint itself (and sweeping NULL rows) is phase 1b.
    status: Literal["active", "archived"] | None = None
    default_config_name: str | None = None
    default_config_override: dict[str, Any] | None = None
    cloud_storage_read_only: bool | None = None
    # Workspace egress tier. Admin-only — see PATCH /api/projects/{id}.
    network_tier: str | None = None


WorkspaceMode = Literal["none", "virtual", "container", "vm"]


class WorkspaceTemplateRef(BaseModel):
    """A template by name and scope, e.g. {kind: Catalog, name: shared}."""

    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1, max_length=63)
    scope: dict[str, str]


class ProjectWorkspaceDefaultsUpdate(BaseModel):
    """Body for PUT /api/projects/{id}/workspace-defaults. A null field falls
    through to the installation."""

    model_config = ConfigDict(extra="forbid")
    jobs: WorkspaceMode | None = None
    sessions: WorkspaceMode | None = None
    container: WorkspaceTemplateRef | None = None
    vm: WorkspaceTemplateRef | None = None


class ProjectConnectorDefaultsUpdate(BaseModel):
    """Body for PUT /api/projects/{id}/connector-defaults: the linked
    connectors new work in the Project attaches when it takes its defaults.
    The project knowledge base is always the first default and is not listed."""

    model_config = ConfigDict(extra="forbid")
    connector_ids: list[UUID] = Field(
        default_factory=list,
        description=(
            "Connector ids (their Connector uids), each linked to the Project, "
            "in order."
        ),
    )


class ProjectMemberAdd(BaseModel):
    """Request body for adding a project member."""

    user_id: str = Field(..., description="User UUID to add")
    role: str = Field("editor", description="Member role: owner, editor, viewer")


class ProjectMemberUpdate(BaseModel):
    """Request body for updating a project member's role."""

    role: str = Field(..., description="New role: owner, editor, viewer")


class ProjectRepositoryCreate(BaseModel):
    """Request body for attaching a repository to a project."""

    name: str = Field(..., description="Repository display name")
    description: str | None = Field(None, description="Repository description")
    repo_url: str | None = Field(None, description="Repository URL (external repos)")
    role: str = Field(
        "source",
        description="Repository role: source or reference",
        pattern="^(source|reference)$",
    )
    read_only: bool = Field(False, description="Whether this repo is read-only")
    branch: str = Field("main", description="Default branch")
    clone_path: str | None = Field(None, description="Local clone path")
    create_managed: bool = Field(False, description="Create a managed Gitea repo")


class ProjectRepositoryUpdate(BaseModel):
    """Request body for updating a project repository."""

    name: str | None = None
    description: str | None = None
    read_only: bool | None = None
    branch: str | None = None
    clone_path: str | None = None


class PromoteRequest(BaseModel):
    """Request body for promoting a job into a dedicated project."""

    name: str = Field(..., description="Name for the new project")
    description: str | None = Field(None, description="Project description")
    goal: str | None = Field(None, description="Project goal")
    user_id: str = Field(..., description="User UUID who owns the new project")
