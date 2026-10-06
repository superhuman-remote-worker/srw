"""Job creation command and its public HTTP documentation projection.

JobCreate preserves the dual public/internal ingress contract. The public body
annotation changes only the documented schema, not parsing or admission: legacy
ignored fields remain accepted, and authenticated internal callers retain their
command fields. Never use this projection to authorize submitted identity.
"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, WithJsonSchema, field_validator, model_validator

from orchestrator.schemas.inline_expert import (
    EXPERT_BASED_ON_DESCRIPTION,
    InlineExpertSelection,
)
from orchestrator.services.job_create_ingress import (
    _SERVER_OWNED_RAW_CREATE_CONTEXT_KEYS,
    _strip_raw_repository_authority,
)


class JobCreate(BaseModel):
    """Request body for creating a new job."""

    description: str = Field(
        ..., description="Job description - what the agent should accomplish"
    )
    upload_id: str | None = Field(
        None, description="Upload ID for document files (from /api/uploads)"
    )
    config_upload_id: str | None = Field(
        None, description="Upload ID for config YAML override"
    )
    instructions_upload_id: str | None = Field(
        None, description="Upload ID for instructions markdown"
    )
    document_path: str | None = Field(
        None, description="Path to a document (deprecated, use upload_id)"
    )
    document_dir: str | None = Field(
        None, description="Directory containing documents (deprecated)"
    )
    expert: str | InlineExpertSelection | None = Field(
        None,
        description=(
            "Which expert runs this job. One selector for the whole catalogue: "
            "either a bundled expert id ('developer') or a DB expert UUID, "
            "exactly as GET /api/experts lists them. Or a complete inline "
            'definition, {"inline": ExpertSpec} with an srw/v1 runtime, which '
            "is frozen with the job and has no catalogue identity. Omit to "
            "accept the deployment's configured default worker. Supersedes "
            "config_name and expert_id, which remain as deprecated single-store "
            "aliases and cannot be combined with an inline expert."
        ),
    )
    expert_based_on: str | None = Field(
        None, description=EXPERT_BASED_ON_DESCRIPTION, max_length=200
    )
    config_name: str = Field(
        "worker_base",
        description=(
            "DEPRECATED alias for `expert` (bundled experts only). Also still "
            "the way to name a non-catalogue deployment config."
        ),
    )
    expert_id: str | None = Field(
        None,
        description=(
            "DEPRECATED alias for `expert` (DB-backed expert UUID only). The "
            "orchestrator resolves it over the worker_base profile."
        ),
    )
    workspace: dict[str, Any] | None = Field(
        None,
        description="Execution workspace: a manifest template binding or null for none. Omit to use Project/default selection.",
    )

    @field_validator("workspace")
    @classmethod
    def _validate_workspace(cls, value):
        from orchestrator.services.manifest_workspace_binding import (
            validate_workspace_selection,
        )

        return validate_workspace_selection(value)

    config_override: dict[str, Any] | None = Field(
        None, description="Per-job configuration overrides"
    )
    context: dict[str, Any] | None = Field(
        None, description="Optional context dictionary"
    )
    instructions: str | None = Field(
        None, description="Additional inline instructions for the agent"
    )
    kickoff_message: str | None = Field(
        None, description="Opening message to the agent (task brief)"
    )
    required_deliverables: list[str] | None = Field(
        None,
        description=(
            "Immutable deliverable contract: workspace-relative artifact "
            "paths, 'kb:<slug>' knowledge notes, or exactly one "
            "'pr:<owner>/<repository>' pull request bound to a writable "
            "attached repository. Files inside repos/<name>/ are refused and "
            "the response names the compatible PR contract. Select that "
            "contract rather than replacing publication with a note."
        ),
    )

    @field_validator("required_deliverables")
    @classmethod
    def _normalize_deliverables(cls, value: list[str] | None) -> list[str] | None:
        """Reject malformed entries while deferring repository authority.

        ``repos/<alias>`` cannot be interpreted until project/ticket scope and
        the exact selected datasource set have been server-resolved. Refusing
        it in Pydantic used to lose the ticket-generation provenance needed to
        prevent a subsequent ``kb:`` downgrade.
        """
        if not value:
            return value
        from shared.deliverable_contract import parse_required_deliverables

        return parse_required_deliverables(value, strict=True)

    ticket: str | None = Field(
        None,
        description=(
            "Backlog ticket (knowledge-note slug) this job claims. Officer "
            "dispatches only. The server resolves its current ready generation "
            "and atomically writes the durable claim plus job; the provenance "
            "is also stamped into context.ticket_note_id. Pass it when working "
            "a ticket by hand so the tick cannot dispatch duplicate work."
        ),
    )
    work_category: str | None = Field(
        None,
        description=(
            "The kind of work this is: researcher (the deliverable is an "
            "ANSWER), tester (the deliverable is issue tickets), or executor "
            "(the deliverable is shipped files). Officer dispatches into a "
            "categorized slot only. The slot's own category always supplies "
            "the contract the worker is held to; naming a different one here "
            "is allowed and is stated in the kickoff rather than refused."
        ),
    )
    datasource_ids: list[str] | None = Field(
        None, description="Connector IDs to attach to the job"
    )
    use_datasource_defaults: bool = Field(
        False,
        description=(
            "Resolve the owner's currently available automatic connector "
            "defaults. Mutually exclusive with datasource_ids."
        ),
    )
    user_id: str | None = Field(None, description="User UUID who created this job")
    project_id: str | None = Field(
        None, description="Project UUID to associate this job with"
    )
    thread_id: str | None = Field(
        None,
        description=(
            "Persistent-session thread UUID. When provided and user_id "
            "is unset, the owning user (and project) are inherited from "
            "the thread row so dispatch can apply user preferences. Also "
            "persisted as jobs.created_by_thread_id, which is what the "
            "completion wake routes on. INTERNAL PATH ONLY — the public path "
            "strips it (_strip_public_job_reserved_markers); a caller cannot "
            "name someone else's session."
        ),
    )
    parent_job_id: str | None = Field(
        None, description="Parent job UUID for verification/follow-up jobs"
    )
    priority: int = Field(
        5, ge=0, le=10, description="Job priority (0=low, 5=normal, 10=high)"
    )
    creation_order: int | None = Field(
        None, description="0-based index for delegation subagent merge ordering"
    )
    worktree_path: str | None = Field(
        None, description="Git worktree path for delegation subagents"
    )
    delegation_context: str | None = Field(
        None, description="Shared context string from parent delegation"
    )
    execution_lane: Literal["pinned", "stateless"] | None = Field(
        None,
        description=(
            "Execution plane opt-in. Omitted root jobs remain pinned; omitted "
            "child jobs inherit their authoritative parent lane. Stateless "
            "worker admission is deployment-gated and Kubernetes-sandbox only."
        ),
    )

    @model_validator(mode="after")
    def reject_null_datasource_selection(self) -> "JobCreate":
        # Earliest ingress fence for both public and internally authenticated
        # HTTP bodies. Completion/provisioning code may mint these namespaces
        # only after resolving server authority. The route helper and Postgres
        # funnel repeat the strip as independent defenses.
        if isinstance(self.context, dict):
            self.context = _strip_raw_repository_authority(
                {
                    key: value
                    for key, value in self.context.items()
                    if key not in _SERVER_OWNED_RAW_CREATE_CONTEXT_KEYS
                }
            )
        if isinstance(self.config_override, dict):
            self.config_override = _strip_raw_repository_authority(self.config_override)
        if "datasource_ids" in self.model_fields_set and self.datasource_ids is None:
            raise ValueError("datasource_ids may be omitted or an array, not null")
        if self.use_datasource_defaults and "datasource_ids" in self.model_fields_set:
            raise ValueError(
                "use_datasource_defaults and datasource_ids are mutually exclusive"
            )
        return self


# Explicit allowlist: adding a future internal command field must not publish
# it to public clients. user_id remains an ignored compatibility input used by
# older Cockpit versions; admission always replaces it with the current caller.
PUBLIC_JOB_CREATE_FIELDS = (
    "description",
    "upload_id",
    "config_upload_id",
    "instructions_upload_id",
    "document_path",
    "document_dir",
    "expert",
    # "expert_based_on" is accepted but published together with the
    # Cockpit's JobCreateRequest field (creation UI slice S3); the projection
    # test holds the two lists equal.
    "config_name",
    "expert_id",
    "config_override",
    "context",
    "instructions",
    "kickoff_message",
    "required_deliverables",
    "datasource_ids",
    "use_datasource_defaults",
    "user_id",
    "project_id",
    "priority",
    "workspace",
    "execution_lane",
)


def _inline_local_definitions(schema: dict[str, Any]) -> dict[str, Any]:
    """Expand ``#/$defs/...`` references so the projection is self-contained.

    The projection is embedded verbatim in the OpenAPI document, where a
    ``#/$defs`` pointer would resolve against the document root and dangle.
    The nested models (the inline expert) are small and acyclic.
    """
    definitions = schema.pop("$defs", {})

    def expand(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                target = expand(definitions[ref.removeprefix("#/$defs/")])
                rest = {key: expand(value) for key, value in node.items()}
                rest.pop("$ref")
                return {**target, **rest}
            return {key: expand(value) for key, value in node.items()}
        if isinstance(node, list):
            return [expand(value) for value in node]
        return node

    return expand(schema)


def public_job_create_schema() -> dict[str, Any]:
    """Describe accepted public inputs while keeping internal parsing intact."""
    schema = _inline_local_definitions(JobCreate.model_json_schema())
    schema["title"] = "PublicJobCreate"
    schema["description"] = (
        "Public job creation. Expert resolution, connector selection, capability "
        "and execution-lane admission remain server/deployment controlled. "
        "Caller identity and internal delegation scope are server-derived."
    )
    fields = schema["properties"]
    schema["properties"] = {key: fields[key] for key in PUBLIC_JOB_CREATE_FIELDS}
    schema["required"] = [
        key for key in schema["required"] if key in PUBLIC_JOB_CREATE_FIELDS
    ]
    for key in ("config_name", "expert_id", "document_path", "document_dir", "user_id"):
        schema["properties"][key]["deprecated"] = True
    schema["properties"]["user_id"]["description"] = (
        "Ignored compatibility input. The authenticated caller always owns the job."
    )
    # The command uses None internally to represent absence, but its validator
    # rejects an explicitly supplied null. Do not publish a nullable wire type
    # or a null default that a client generator would send back to the server.
    datasource = schema["properties"]["datasource_ids"]
    array = next(
        branch for branch in datasource.pop("anyOf") if branch["type"] == "array"
    )
    datasource.update(array)
    datasource.pop("default", None)
    schema["not"] = {
        "required": ["datasource_ids", "use_datasource_defaults"],
        "properties": {"use_datasource_defaults": {"const": True}},
    }
    return schema


PublicJobCreateBody = Annotated[JobCreate, WithJsonSchema(public_job_create_schema())]
