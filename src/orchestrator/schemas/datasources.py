"""Datasource and repository connector API contracts."""

from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator
from shared.runtime.core.datasource_catalog import DATASOURCE_TYPE_IDS


class DatasourceCreate(BaseModel):
    """Request body for creating a connector through the datasource API."""

    name: str = Field(..., description="User-provided label")
    type: str = Field(
        ...,
        description=f"Connector type: {', '.join(DATASOURCE_TYPE_IDS)}",
    )
    connection_url: str | None = Field(
        None, description="Connection string (nullable for generic)"
    )
    description: str | None = Field(None, description="What this connector contains")
    credentials: dict[str, Any] | None = Field(
        None,
        description="Auth details (env_vars for generic, auth_method+token/ssh_key for repository, type-specific for managed)",
    )
    job_id: str | None = Field(None, description="Job UUID (null for global)")
    scope_mode: Literal["all", "projects"] = Field(
        "all",
        description="Execution availability: everywhere or selected projects",
    )
    project_ids: list[str] | None = Field(
        None,
        description="Full project scope when scope_mode is 'projects'",
    )
    auto_attach: bool = Field(
        False,
        description="Select this connector by default in the owner's new work",
    )
    cli_hint: str | None = Field(
        None, description="Suggested CLI command (e.g. 'psql $DATABASE_URL')"
    )
    default_branch: str | None = Field(
        None, description="Branch to read/clone (repository and kb types)"
    )
    config: dict[str, Any] | None = Field(
        None,
        description=(
            "Non-secret type-specific config (kb: root_path; email: access/"
            "folders/drafts_folder/from_address/recipient_allowlist/"
            "unattended_send)"
        ),
    )
    is_global: bool = Field(
        False, description="Whether this connector is visible to all users"
    )
    read_only: bool | None = Field(
        None,
        description=(
            "Declared read-only flag for public connectors (defaults to true "
            "on publish; kb is always read-only). Declarative — credentials "
            "are the enforcement boundary."
        ),
    )

    @model_validator(mode="after")
    def validate_availability_policy(self) -> "DatasourceCreate":
        if "project_ids" in self.model_fields_set and self.project_ids is None:
            raise ValueError("project_ids may be omitted or an array, not null")
        if self.scope_mode == "projects" and not self.project_ids:
            raise ValueError("project_ids is required for project-scoped connectors")
        if self.scope_mode == "all" and self.project_ids:
            raise ValueError("project_ids requires scope_mode='projects'")
        return self


class DatasourceUpdate(BaseModel):
    """Request body for updating a connector through the datasource API."""

    name: str | None = Field(None, description="New label")
    description: str | None = Field(None, description="New description")
    connection_url: str | None = Field(None, description="New connection string")
    credentials: dict[str, Any] | None = Field(None, description="New auth details")
    cli_hint: str | None = Field(None, description="New CLI hint")
    default_branch: str | None = Field(None, description="New default branch")
    config: dict[str, Any] | None = Field(
        None,
        description=(
            "New non-secret type-specific config (kb: root_path; email: "
            "access/folders/drafts_folder/from_address/recipient_allowlist/"
            "unattended_send)"
        ),
    )
    is_global: bool | None = Field(
        None,
        description=(
            "Publish (true) or unpublish (false). Publishing requires the "
            "'public_datasources' capability; unpublishing needs only "
            "creator/admin."
        ),
    )
    read_only: bool | None = Field(
        None,
        description="Declared read-only flag (kb: always true; declarative only)",
    )
    scope_mode: Literal["all", "projects"] | None = Field(
        None, description="New execution availability mode"
    )
    project_ids: list[str] | None = Field(
        None, description="Desired full project scope; omission preserves links"
    )
    auto_attach: bool | None = Field(
        None, description="New owner-specific default-selection preference"
    )
    policy_revision: int | None = Field(
        None, ge=1, description="Optimistic concurrency token for policy edits"
    )

    @model_validator(mode="after")
    def validate_availability_policy(self) -> "DatasourceUpdate":
        policy_fields = {"scope_mode", "project_ids", "auto_attach"}
        changed = policy_fields.intersection(self.model_fields_set)
        for field_name in changed | ({"policy_revision"} & self.model_fields_set):
            if getattr(self, field_name) is None:
                raise ValueError(f"{field_name} may be omitted, but not null")
        if changed and self.policy_revision is None:
            raise ValueError("policy_revision is required for availability changes")
        return self


class DatasourceTestRequest(BaseModel):
    """Optional body of ``POST /api/datasources/{id}/test``.

    The connector form tests before it saves. For an SSH-key repository or an
    ``ssh_key`` connector, the endpoint being typed (``connection_url`` and
    ``config``) is what Test reaches; it is validated like an update and never
    stored. Other connector types ignore it and test the saved row.
    """

    model_config = ConfigDict(extra="forbid")

    connection_url: str | None = Field(None, max_length=2048)
    config: dict[str, Any] | None = None


class SSHKeyGenerateRequest(BaseModel):
    """Request body for generating an SSH keypair for a repository connector."""

    comment: str | None = Field(
        None,
        description="Optional comment to embed in the public key (e.g. connector name)",
        max_length=200,
    )


class SSHKeyGenerateResponse(BaseModel):
    """Response containing a freshly generated ed25519 SSH keypair."""

    private_key: str = Field(..., description="OpenSSH PEM private key (no passphrase)")
    public_key: str = Field(
        ..., description="Single-line OpenSSH public key for the deploy-keys field"
    )


class ProjectDatasourceSettings(BaseModel):
    """Project-level settings when linking a connector."""

    read_only: bool | None = Field(
        None,
        description="Managed connectors: true = read-only tools, false/null = CLI mode",
    )
    description: str | None = Field(None, description="Project-specific usage context")
