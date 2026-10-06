"""Request models for persistent-session admission.

Extracted verbatim from ``orchestrator.main`` (R1.B06 lane B, census group
``S_ADMISSION``). Class names are the wire contract — ``ThreadCreateRequest``
is the declared body of ``POST /api/persistent/threads`` and appears under that
name in the published OpenAPI document — so they move unchanged.

Two properties are load-bearing and are preserved exactly:

* **The three ``PrivateAttr`` seeds are server-only.** Pydantic never populates
  a private attribute from JSON, so neither the public create endpoint nor the
  model-facing MCP tool can author ``_trusted_seed``,
  ``_officer_post_config_snapshot`` or ``_officer_commission_result``. They are
  the only bridges for review-delivery context and durable Officer Post
  authority into the shared create funnel, and moving them to ordinary fields
  would hand a client that authority.
* **Both validators refuse rather than normalise.** ``execution_lane`` is
  rejected at the public boundary so session creation stays topology-neutral;
  a ``datasource_ids: null`` and the ``use_datasource_defaults`` conflict are
  refused instead of being coerced, because an empty selection is
  authoritative (see [[reference_empty_datasource_ids_is_authoritative]]).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import (
    field_validator,
    BaseModel,
    Field,
    PrivateAttr,
    StrictBool,
    model_validator,
)


@dataclass(frozen=True)
class TrustedThreadSeed:
    """Server-authored context committed before a new thread can attach."""

    metadata: dict[str, Any]
    opening_event: str


class ThreadCreateRequest(BaseModel):
    """Request body for creating a persistent thread."""

    # Sessions run the persistent base config — every other session-config
    # fallback in this file already says "session_base". The old
    # "defaults" default silently put bare API threads on the WORKER yaml
    # (knowledge-base/knowledge/issues/session_config_name_plumbing.md, hole A).
    config_name: str = Field("session_base", description="Agent config to use")
    project_id: str | None = Field(
        None,
        description=(
            "The Session's project UUID. A Session has one project or none; "
            "omit for none."
        ),
    )
    project_ids: list[str] | None = Field(
        None,
        description=(
            "Deprecated: use project_id. At most one project UUID; more than "
            "one distinct project is refused with 422."
        ),
    )
    datasource_ids: list[str] | None = Field(
        None, description="Explicit connector IDs to attach to this thread"
    )
    use_datasource_defaults: bool = Field(
        False,
        description=(
            "Resolve the owner's currently available automatic connector "
            "defaults. Mutually exclusive with datasource_ids."
        ),
    )
    permission_mode: str | None = Field(
        None,
        description=(
            "Per-session permission mode override. Omit to inherit the user's "
            "saved default, then the config default ('supervised')."
        ),
    )
    title: str = Field("Untitled Session", description="Session title")
    expert_id: str | None = Field(
        None,
        description=(
            "DB-backed expert UUID for this session. Preferred over config_name "
            "for expert selection — stored in metadata.expert_id and resolved "
            "into the session config at attach. config_name stays the base."
        ),
    )
    model: str | None = Field(
        None,
        description="LLM model override (e.g. RedHatAI/gemma-4-31B-it-FP8-Dynamic)",
    )
    temperature: float | None = Field(None, description="Temperature override")
    reasoning_level: str | None = Field(
        None,
        description=(
            "Per-session reasoning-effort override (low|medium|high|xhigh|max|"
            "none). Omit to inherit the account default, then the family "
            "default. Clamped to the model family's supported levels at "
            "attach (model_config_matrix.yaml)."
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
        None,
        description=(
            "Per-session config overrides from the New Session 'Advanced' form. "
            "The workspace sub-dict is honored at create time: workspace.backend "
            "selects the tier (sandbox | virtual | none) and MUST be set here "
            "because the workspace is provisioned at creation. vm is not "
            "creatable directly — start on a lite tier and upgrade. The "
            "tools.orchestrator and tools.agent_catalog categories are also "
            "honored as session tool group toggles."
        ),
    )
    protected_cloud: StrictBool = Field(
        False,
        description=(
            "Protected cloud mode: mount the project cloud folder read-only with "
            "a capture overlay so agent writes are staged for review, not live. "
            "Nextcloud-only, container-runtime-only (design §3, §9.2). The New "
            "Session checkbox that sets this lands in Slice C."
        ),
    )

    # Server-only creation context. Pydantic private attributes are never
    # populated from JSON, so neither the public thread-create endpoint nor the
    # model-facing MCP tool can author this seed. The job review endpoint sets
    # it only after deriving the delivery from an access-checked job id.
    _trusted_seed: "TrustedThreadSeed | None" = PrivateAttr(default=None)
    # Explicit Officer commission provisions a thread outside the post
    # transaction, then atomically registers it. This server-only snapshot
    # prevents a concurrent post edit from being overwritten at registration;
    # JSON callers cannot populate a Pydantic private attribute.
    _officer_post_config_snapshot: "dict[str, Any] | None" = PrivateAttr(default=None)
    # Filled only by the authoritative registration transaction for the
    # explicit commission endpoint. It carries the already-persisted
    # continuity outcome back through the shared create funnel without
    # exposing a client-authored field.
    _officer_commission_result: "dict[str, Any] | None" = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def reject_execution_lane_selector(cls, value: Any) -> Any:
        """Keep session creation topology-neutral at the public boundary."""
        if isinstance(value, dict) and "execution_lane" in value:
            raise ValueError(
                "execution_lane is orchestrator-managed and cannot be selected"
            )
        return value

    @model_validator(mode="after")
    def reject_null_datasource_selection(self) -> "ThreadCreateRequest":
        if "datasource_ids" in self.model_fields_set and self.datasource_ids is None:
            raise ValueError("datasource_ids may be omitted or an array, not null")
        if self.use_datasource_defaults and "datasource_ids" in self.model_fields_set:
            raise ValueError(
                "use_datasource_defaults and datasource_ids are mutually exclusive"
            )
        return self


class ThreadUpdateRequest(BaseModel):
    """Request body for updating a persistent thread's mutable metadata."""

    title: str | None = Field(None, description="New session title")


__all__ = [
    "ThreadCreateRequest",
    "ThreadUpdateRequest",
    "TrustedThreadSeed",
]
