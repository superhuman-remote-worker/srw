"""Typed product-capability contract and build registry.

This module is deliberately framework- and I/O-free.  It defines the public
v1 vocabulary, the immutable build-level registry, deterministic registry
revision helpers, and pure safety/validation helpers shared by the
orchestrator and persistent agent.

Dynamic deployment, user, project, and live-session resolution belongs to the
later M2 server/tool slices.  Importing this module never reads guide files,
queries a database, inspects environment flags, or infers runtime availability.

Design:
    knowledge-base/knowledge/features/app_guide_skill.md
    knowledge-base/knowledge/superpowers/plans/2026-07-27-app-guide-m2.md
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Any, Literal, TypeAlias
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from shared.permission_modes import PERMISSION_MODES

SCHEMA_VERSION = "1.1"
SCHEMA_MAJOR = 1

MAX_CAPABILITY_ID_LENGTH = 120
MAX_TOPIC_ID_LENGTH = 80
MAX_DEFINITION_TOPICS = 8
MAX_PUBLIC_QUALIFIER_KEYS = 8
MAX_LAYER_QUALIFIERS = 8
MAX_QUALIFIER_LIST_ITEMS = 8
MAX_CAPABILITIES_PER_RESPONSE = 50
MAX_EVALUATION_ERRORS = 64

_SCHEMA_VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
_QUALIFIER_TOKEN_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,79}$")

CapabilityId = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=MAX_CAPABILITY_ID_LENGTH,
        pattern=r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+$",
    ),
]
TopicId = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=MAX_TOPIC_ID_LENGTH,
        pattern=r"^[a-z0-9][a-z0-9-]*$",
    ),
]
GuideReference = Annotated[
    str,
    StringConstraints(
        min_length=len("references/a.md"),
        max_length=120,
        pattern=r"^references/[a-z0-9][a-z0-9-]{0,79}\.md$",
    ),
]
TranslationKey = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=160,
        pattern=r"^[A-Za-z][A-Za-z0-9_-]*(?:\.[A-Za-z0-9_-]+)+$",
    ),
]
ActionId = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=MAX_CAPABILITY_ID_LENGTH,
        pattern=r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+$",
    ),
]
QualifierKey = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=80,
        pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$",
    ),
]
Sha256Digest = Annotated[
    str,
    StringConstraints(pattern=r"^sha256:[a-f0-9]{64}$"),
]
FullSourceRevision = Annotated[
    str,
    StringConstraints(pattern=r"^[a-f0-9]{40,64}$"),
]


class _FrozenModel(BaseModel):
    """Closed, immutable base for public contract models."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class SchemaCompatibility(str, Enum):
    """How an observed schema version relates to this client."""

    EXACT = "exact"
    SAME_MAJOR = "same_major"
    UNSUPPORTED_MAJOR = "unsupported_major"
    INVALID = "invalid"


class CapabilityVisibility(str, Enum):
    PUBLIC = "public"
    CONDITIONAL = "conditional"
    HIDDEN = "hidden"


class Completeness(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"


class BuildState(str, Enum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class DeploymentState(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


class UserState(str, Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    NO_OPINION = "no_opinion"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class SessionState(str, Enum):
    READY = "ready"
    NOT_READY = "not_ready"
    NEEDS_ATTACHMENT = "needs_attachment"
    NEEDS_UPGRADE = "needs_upgrade"
    DEGRADED = "degraded"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"


class AgentAction(str, Enum):
    CAN_EXECUTE = "can_execute"
    CAN_PROPOSE = "can_propose"
    CAN_GUIDE = "can_guide"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class Freshness(str, Enum):
    FRESH = "fresh"
    STALE = "stale"
    UNKNOWN = "unknown"


class ProvenanceStatus(str, Enum):
    DECLARED = "declared"
    VERIFIED = "verified"
    UNAVAILABLE = "unavailable"


class ScopeKind(str, Enum):
    USER = "user"
    THREAD = "thread"


class LayerName(str, Enum):
    BUILD = "build"
    DEPLOYMENT = "deployment"
    USER = "user"
    SESSION = "session"


class ProductComponent(str, Enum):
    """Known first-party observers/artifacts in schema 1.x."""

    REGISTRY = "registry"
    ORCHESTRATOR = "orchestrator"
    AGENT = "agent"
    COCKPIT = "cockpit"
    GUIDE = "guide"
    WORKSPACE = "workspace"
    MCP = "mcp"


class CapabilityResolverKey(str, Enum):
    """Closed reviewed resolver adapters for the initial registry."""

    AUTOMATIONS_MANAGE = "automations_manage"
    CANVAS_BROWSER = "canvas_browser"
    CANVAS_FILES = "canvas_files"
    DATASOURCE_EMAIL = "datasource_email"
    DATASOURCE_EMAIL_SEND = "datasource_email_send"
    DATASOURCE_OKF = "datasource_okf"
    EXPERTS_MANAGE = "experts_manage"
    EXPERTS_SELECT = "experts_select"
    JOBS_CREATE = "jobs_create"
    JOBS_REVIEW = "jobs_review"
    MEMORY_RECALL = "memory_recall"
    PROJECT_KNOWLEDGE = "project_knowledge"
    PROJECT_LOOPS = "project_loops"
    PROJECTS_MANAGE = "projects_manage"
    SESSION_DELEGATE = "session_delegate"
    SESSION_PERMISSION_MODE = "session_permission_mode"
    SESSION_PROTECTED_CLOUD = "session_protected_cloud"
    WORKSPACE_SELECT = "workspace_select"


class ReasonCode(str, Enum):
    """Stable, public-safe per-layer reason vocabulary."""

    # Build
    INCLUDED_IN_BUILD = "included_in_build"
    NOT_IN_BUILD = "not_in_build"
    BUILD_OBSERVATION_UNAVAILABLE = "build_observation_unavailable"

    # Deployment
    FEATURE_ENABLED = "feature_enabled"
    FEATURE_DISABLED = "feature_disabled"
    CONNECTOR_TYPE_AVAILABLE = "connector_type_available"
    PROVIDER_NOT_CONFIGURED = "provider_not_configured"
    SERVICE_DEGRADED = "service_degraded"
    DEPLOYMENT_OBSERVATION_UNAVAILABLE = "deployment_observation_unavailable"

    # User/policy
    ADMIN_ALLOWED = "admin_allowed"
    APPROVED_USER = "approved_user"
    GRANT_ALLOWED = "grant_allowed"
    GRANT_DENIED = "grant_denied"
    NO_POLICY_OPINION = "no_policy_opinion"
    USER_NOT_APPLICABLE = "user_not_applicable"
    USER_OBSERVATION_UNAVAILABLE = "user_observation_unavailable"

    # Session/live agent
    THREAD_NOT_REQUESTED = "thread_not_requested"
    LIVE_AGENT_OBSERVATION_REQUIRED = "live_agent_observation_required"
    TOOL_LOADED = "tool_loaded"
    TOOL_NOT_LOADED = "tool_not_loaded"
    DATASOURCE_ATTACHED = "datasource_attached"
    DATASOURCE_NOT_ATTACHED = "datasource_not_attached"
    DATASOURCE_CONNECTION_DEGRADED = "datasource_connection_degraded"
    WORKSPACE_READY = "workspace_ready"
    WORKSPACE_REQUIRED = "workspace_required"
    WORKSPACE_NOT_READY = "workspace_not_ready"
    KNOWLEDGE_READY = "knowledge_ready"
    KNOWLEDGE_UNAVAILABLE = "knowledge_unavailable"
    CLOUD_ATTACHED = "cloud_attached"
    CLOUD_NOT_ATTACHED = "cloud_not_attached"
    SESSION_OBSERVATION_UNAVAILABLE = "session_observation_unavailable"


class EvaluationErrorCode(str, Enum):
    RESOLVER_ERROR = "resolver_error"
    STALE_OBSERVATION = "stale_observation"
    RESULT_LIMIT = "result_limit"
    RESPONSE_SIZE_LIMIT = "response_size_limit"
    CAPABILITY_NOT_VISIBLE = "capability_not_visible"


class CapabilityComponents(_FrozenModel):
    """Components relevant to one user-visible capability."""

    authority: ProductComponent
    execution: ProductComponent
    presentation: ProductComponent
    guidance: ProductComponent

    @model_validator(mode="after")
    def validate_component_roles(self) -> CapabilityComponents:
        if self.authority is ProductComponent.REGISTRY:
            raise ValueError("registry cannot be a runtime authority component")
        if self.guidance is not ProductComponent.GUIDE:
            raise ValueError("guidance component must be guide")
        return self


PUBLIC_QUALIFIER_KEYS = frozenset(
    {
        "email.access_tier",
        "permission_mode.ceiling",
        "workspace.backend",
        "workspace.supports_shell",
    }
)

_EMAIL_ACCESS_TIERS = frozenset({"read", "read_write", "draft", "send"})
_PERMISSION_MODE_CEILINGS = frozenset(PERMISSION_MODES)
_WORKSPACE_BACKENDS = frozenset({"sandbox", "vm", "virtual", "none"})


class CapabilityQualifier(_FrozenModel):
    """One bounded, registry-allowlisted public fact.

    Free-form text and nested objects are intentionally unsupported.  Current
    keys have key-specific value contracts so a future resolver cannot use a
    nominally safe key as a covert arbitrary-text channel.
    """

    key: QualifierKey
    value: str | bool | int | tuple[str, ...]

    @field_validator("value", mode="before")
    @classmethod
    def validate_raw_value(cls, value: Any) -> str | bool | int | tuple[str, ...]:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            if abs(value) > 1_000_000_000:
                raise ValueError("qualifier integer is out of bounds")
            return value
        if isinstance(value, str):
            if not _QUALIFIER_TOKEN_RE.fullmatch(value):
                raise ValueError("qualifier strings must be bounded enum-like tokens")
            return value
        if isinstance(value, (list, tuple)):
            if not 1 <= len(value) <= MAX_QUALIFIER_LIST_ITEMS:
                raise ValueError("qualifier token list is out of bounds")
            if not all(
                isinstance(item, str) and _QUALIFIER_TOKEN_RE.fullmatch(item)
                for item in value
            ):
                raise ValueError(
                    "qualifier lists may contain only bounded enum-like tokens"
                )
            if len(set(value)) != len(value):
                raise ValueError("qualifier token lists may not contain duplicates")
            return tuple(value)
        raise ValueError("unsupported qualifier value type")

    @model_validator(mode="after")
    def validate_key_value_contract(self) -> CapabilityQualifier:
        if self.key not in PUBLIC_QUALIFIER_KEYS:
            raise ValueError(f"unregistered public qualifier key: {self.key}")
        if self.key == "email.access_tier":
            if not isinstance(self.value, str) or self.value not in _EMAIL_ACCESS_TIERS:
                raise ValueError("email.access_tier has an invalid value")
        elif self.key == "permission_mode.ceiling":
            if (
                not isinstance(self.value, str)
                or self.value not in _PERMISSION_MODE_CEILINGS
            ):
                raise ValueError("permission_mode.ceiling has an invalid value")
        elif self.key == "workspace.backend":
            if not isinstance(self.value, str) or self.value not in _WORKSPACE_BACKENDS:
                raise ValueError("workspace.backend has an invalid value")
        elif self.key == "workspace.supports_shell":
            if not isinstance(self.value, bool):
                raise ValueError("workspace.supports_shell must be boolean")
        return self


class CapabilityDefinition(_FrozenModel):
    """Immutable public metadata for one registered capability."""

    id: CapabilityId
    topics: tuple[TopicId, ...] = Field(min_length=1, max_length=MAX_DEFINITION_TOPICS)
    components: CapabilityComponents
    visibility: CapabilityVisibility
    title_key: TranslationKey
    summary: str = Field(min_length=1, max_length=240)
    help_topic_id: TopicId
    guide_ref: GuideReference
    resolver_key: CapabilityResolverKey
    public_qualifier_keys: tuple[QualifierKey, ...] = Field(
        default_factory=tuple,
        max_length=MAX_PUBLIC_QUALIFIER_KEYS,
    )
    open_action_id: ActionId | None = None
    visual_id: ActionId | None = None

    @field_validator("summary")
    @classmethod
    def validate_summary(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("summary may not have leading/trailing whitespace")
        if any(ord(char) < 32 for char in value):
            raise ValueError("summary may not contain control characters")
        return value

    @field_validator("topics", "public_qualifier_keys")
    @classmethod
    def validate_unique_sorted_tokens(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("definition token lists may not contain duplicates")
        if tuple(sorted(value)) != value:
            raise ValueError("definition token lists must be sorted")
        return value

    @model_validator(mode="after")
    def validate_definition_links(self) -> CapabilityDefinition:
        expected_ref = f"references/{self.help_topic_id}.md"
        if self.guide_ref != expected_ref:
            raise ValueError(
                f"guide_ref must match the logical help_topic_id ({expected_ref!r})"
            )
        unexpected_keys = set(self.public_qualifier_keys) - PUBLIC_QUALIFIER_KEYS
        if unexpected_keys:
            raise ValueError(
                "definition declares unregistered qualifier keys: "
                + ", ".join(sorted(unexpected_keys))
            )
        return self


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def _validate_qualifier_tuple(
    qualifiers: tuple[CapabilityQualifier, ...],
) -> tuple[CapabilityQualifier, ...]:
    keys = [qualifier.key for qualifier in qualifiers]
    if len(set(keys)) != len(keys):
        raise ValueError("a layer may emit each qualifier key at most once")
    if tuple(sorted(keys)) != tuple(keys):
        raise ValueError("layer qualifiers must be sorted by key")
    return qualifiers


class _LayerEvaluation(_FrozenModel):
    reason_code: ReasonCode
    source_component: ProductComponent
    freshness: Freshness
    observed_at: datetime
    qualifiers: tuple[CapabilityQualifier, ...] = Field(
        default_factory=tuple,
        max_length=MAX_LAYER_QUALIFIERS,
    )

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="observed_at")

    @field_validator("qualifiers")
    @classmethod
    def validate_qualifiers(
        cls, value: tuple[CapabilityQualifier, ...]
    ) -> tuple[CapabilityQualifier, ...]:
        return _validate_qualifier_tuple(value)


class BuildEvaluation(_LayerEvaluation):
    state: BuildState

    @model_validator(mode="after")
    def validate_reason(self) -> BuildEvaluation:
        validate_reason_code(LayerName.BUILD, self.state, self.reason_code)
        return self


class DeploymentEvaluation(_LayerEvaluation):
    state: DeploymentState

    @model_validator(mode="after")
    def validate_reason(self) -> DeploymentEvaluation:
        validate_reason_code(LayerName.DEPLOYMENT, self.state, self.reason_code)
        return self


class UserEvaluation(_LayerEvaluation):
    state: UserState

    @model_validator(mode="after")
    def validate_reason(self) -> UserEvaluation:
        validate_reason_code(LayerName.USER, self.state, self.reason_code)
        return self


class SessionEvaluation(_LayerEvaluation):
    state: SessionState

    @model_validator(mode="after")
    def validate_reason(self) -> SessionEvaluation:
        validate_reason_code(LayerName.SESSION, self.state, self.reason_code)
        return self


LayerEvaluation: TypeAlias = (
    BuildEvaluation | DeploymentEvaluation | UserEvaluation | SessionEvaluation
)
LayerState: TypeAlias = BuildState | DeploymentState | UserState | SessionState


REASON_CODE_COMPATIBILITY: Mapping[
    LayerName, Mapping[LayerState, frozenset[ReasonCode]]
] = MappingProxyType(
    {
        LayerName.BUILD: MappingProxyType(
            {
                BuildState.SUPPORTED: frozenset({ReasonCode.INCLUDED_IN_BUILD}),
                BuildState.UNSUPPORTED: frozenset({ReasonCode.NOT_IN_BUILD}),
                BuildState.UNKNOWN: frozenset(
                    {ReasonCode.BUILD_OBSERVATION_UNAVAILABLE}
                ),
            }
        ),
        LayerName.DEPLOYMENT: MappingProxyType(
            {
                DeploymentState.ENABLED: frozenset(
                    {
                        ReasonCode.FEATURE_ENABLED,
                        ReasonCode.CONNECTOR_TYPE_AVAILABLE,
                    }
                ),
                DeploymentState.DISABLED: frozenset(
                    {
                        ReasonCode.FEATURE_DISABLED,
                        ReasonCode.PROVIDER_NOT_CONFIGURED,
                    }
                ),
                DeploymentState.DEGRADED: frozenset({ReasonCode.SERVICE_DEGRADED}),
                DeploymentState.UNKNOWN: frozenset(
                    {ReasonCode.DEPLOYMENT_OBSERVATION_UNAVAILABLE}
                ),
            }
        ),
        LayerName.USER: MappingProxyType(
            {
                UserState.ALLOWED: frozenset(
                    {
                        ReasonCode.ADMIN_ALLOWED,
                        ReasonCode.APPROVED_USER,
                        ReasonCode.GRANT_ALLOWED,
                    }
                ),
                UserState.DENIED: frozenset({ReasonCode.GRANT_DENIED}),
                UserState.NO_OPINION: frozenset({ReasonCode.NO_POLICY_OPINION}),
                UserState.NOT_APPLICABLE: frozenset({ReasonCode.USER_NOT_APPLICABLE}),
                UserState.UNKNOWN: frozenset({ReasonCode.USER_OBSERVATION_UNAVAILABLE}),
            }
        ),
        LayerName.SESSION: MappingProxyType(
            {
                SessionState.READY: frozenset(
                    {
                        ReasonCode.TOOL_LOADED,
                        ReasonCode.DATASOURCE_ATTACHED,
                        ReasonCode.WORKSPACE_READY,
                        ReasonCode.KNOWLEDGE_READY,
                        ReasonCode.CLOUD_ATTACHED,
                    }
                ),
                SessionState.NOT_READY: frozenset(
                    {
                        ReasonCode.TOOL_NOT_LOADED,
                        ReasonCode.WORKSPACE_NOT_READY,
                        ReasonCode.KNOWLEDGE_UNAVAILABLE,
                        ReasonCode.CLOUD_NOT_ATTACHED,
                    }
                ),
                SessionState.NEEDS_ATTACHMENT: frozenset(
                    {ReasonCode.DATASOURCE_NOT_ATTACHED}
                ),
                SessionState.NEEDS_UPGRADE: frozenset({ReasonCode.WORKSPACE_REQUIRED}),
                SessionState.DEGRADED: frozenset(
                    {ReasonCode.DATASOURCE_CONNECTION_DEGRADED}
                ),
                SessionState.NOT_APPLICABLE: frozenset(
                    {ReasonCode.THREAD_NOT_REQUESTED}
                ),
                SessionState.UNKNOWN: frozenset(
                    {
                        ReasonCode.LIVE_AGENT_OBSERVATION_REQUIRED,
                        ReasonCode.SESSION_OBSERVATION_UNAVAILABLE,
                    }
                ),
            }
        ),
    }
)


def validate_reason_code(
    layer: LayerName, state: LayerState, reason_code: ReasonCode
) -> None:
    """Raise when a public layer state/reason pair is contradictory."""

    allowed_by_state = REASON_CODE_COMPATIBILITY.get(layer)
    allowed = allowed_by_state.get(state) if allowed_by_state else None
    if allowed is None or reason_code not in allowed:
        state_value = getattr(state, "value", str(state))
        raise ValueError(
            f"reason code {reason_code.value!r} is invalid for "
            f"{layer.value}.{state_value}"
        )


class ComponentProvenance(_FrozenModel):
    """Declared/verified identity for one running component or guide bundle."""

    source_revision: FullSourceRevision | None = None
    source_url: str | None = Field(default=None, min_length=1, max_length=2048)
    artifact_digest: Sha256Digest | None = None
    content_digest: Sha256Digest | None = None
    release_version: str | None = Field(default=None, min_length=1, max_length=80)
    documentation_url: str | None = Field(
        default=None,
        min_length=1,
        max_length=2048,
    )
    provenance_status: ProvenanceStatus

    @field_validator("source_url", "documentation_url")
    @classmethod
    def validate_public_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value != value.strip() or not value.startswith("https://"):
            raise ValueError("provenance URL must be an absolute HTTPS URL")
        if "@" in value.split("://", 1)[1].split("/", 1)[0]:
            raise ValueError("provenance URL may not contain URL credentials")
        return value

    @field_validator("release_version")
    @classmethod
    def validate_release_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value != value.strip() or not _QUALIFIER_TOKEN_RE.fullmatch(value):
            raise ValueError("release_version must be a bounded token")
        return value

    @model_validator(mode="after")
    def validate_provenance_evidence(self) -> ComponentProvenance:
        all_metadata = (
            self.source_revision,
            self.source_url,
            self.artifact_digest,
            self.content_digest,
            self.release_version,
            self.documentation_url,
        )
        if self.provenance_status is ProvenanceStatus.UNAVAILABLE:
            if any(item is not None for item in all_metadata):
                raise ValueError("unavailable provenance may not carry evidence")
            return self
        if not any(
            item is not None
            for item in (
                self.source_revision,
                self.artifact_digest,
                self.content_digest,
                self.release_version,
            )
        ):
            raise ValueError("declared/verified provenance requires identity evidence")
        if self.source_url is not None and self.source_revision is None:
            raise ValueError("source_url requires a full source_revision")
        if self.provenance_status is ProvenanceStatus.VERIFIED and (
            self.source_revision is None or self.artifact_digest is None
        ):
            raise ValueError(
                "verified provenance requires source_revision and artifact_digest"
            )
        return self


class ProductProvenance(_FrozenModel):
    """Product identity plus independently deployable component identities."""

    name: Literal["Superhuman Remote Worker"] = "Superhuman Remote Worker"
    release_version: str | None = Field(default=None, min_length=1, max_length=80)
    mixed_build: bool | None = None
    components: dict[ProductComponent, ComponentProvenance] = Field(
        default_factory=dict,
        max_length=len(ProductComponent),
    )

    @field_validator("release_version")
    @classmethod
    def validate_release_version(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value != value.strip() or not _QUALIFIER_TOKEN_RE.fullmatch(value):
            raise ValueError("release_version must be a bounded token")
        return value

    @model_validator(mode="after")
    def validate_mixed_build(self) -> ProductProvenance:
        revisions = [
            component.source_revision
            for component in self.components.values()
            if component.source_revision is not None
        ]
        if self.mixed_build is None:
            return self
        if len(revisions) < 2:
            raise ValueError(
                "mixed_build must be null until at least two revisions are known"
            )
        distinct = len(set(revisions))
        if self.mixed_build is True and distinct < 2:
            raise ValueError("mixed_build=true requires conflicting revisions")
        if self.mixed_build is False and distinct != 1:
            raise ValueError("mixed_build=false requires matching revisions")
        return self


class CapabilityScope(_FrozenModel):
    kind: ScopeKind
    thread_id: UUID | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> CapabilityScope:
        if self.kind is ScopeKind.USER and self.thread_id is not None:
            raise ValueError("user scope may not include thread_id")
        if self.kind is ScopeKind.THREAD and self.thread_id is None:
            raise ValueError("thread scope requires thread_id")
        return self


class EvaluationError(_FrozenModel):
    """Bounded public-safe error; deliberately has no message/detail field."""

    code: EvaluationErrorCode
    capability_id: CapabilityId | None = None
    layer: LayerName | None = None
    source_component: ProductComponent
    retryable: bool

    @model_validator(mode="after")
    def validate_error_shape(self) -> EvaluationError:
        if self.code in {
            EvaluationErrorCode.RESOLVER_ERROR,
            EvaluationErrorCode.STALE_OBSERVATION,
        }:
            if self.capability_id is None or self.layer is None:
                raise ValueError(
                    "resolver/stale errors require capability_id and layer"
                )
        elif self.layer is not None:
            raise ValueError("request/limit errors may not name a layer")
        if (
            self.code is EvaluationErrorCode.CAPABILITY_NOT_VISIBLE
            and self.capability_id is not None
        ):
            raise ValueError(
                "capability_not_visible may not reveal the requested capability ID"
            )
        return self


class ProductCapability(_FrozenModel):
    """One effective per-layer capability observation."""

    id: CapabilityId
    visibility: CapabilityVisibility
    build: BuildEvaluation
    deployment: DeploymentEvaluation
    user: UserEvaluation
    session: SessionEvaluation
    agent_action: AgentAction
    help_topic_id: TopicId
    open_action_id: ActionId | None = None
    visual_id: ActionId | None = None

    @model_validator(mode="after")
    def validate_positive_actionability(self) -> ProductCapability:
        if (
            self.session.state is SessionState.NOT_APPLICABLE
            and self.agent_action is not AgentAction.UNKNOWN
        ):
            raise ValueError("a non-thread result requires agent_action=unknown")
        upper_layers_allow = (
            self.build.state is BuildState.SUPPORTED
            and self.deployment.state is DeploymentState.ENABLED
            and self.user.state is UserState.ALLOWED
        )
        if self.agent_action is AgentAction.CAN_EXECUTE and (
            not upper_layers_allow or self.session.state is not SessionState.READY
        ):
            raise ValueError(
                "can_execute requires supported/enabled/allowed/ready layers"
            )
        if self.agent_action is AgentAction.CAN_PROPOSE and not upper_layers_allow:
            raise ValueError(
                "can_propose requires supported/enabled/allowed upper layers"
            )
        return self


class ProductCapabilitiesResponse(_FrozenModel):
    """Schema-1.1 endpoint/tool result envelope."""

    schema_version: Literal["1.1"] = SCHEMA_VERSION
    registry_revision: Sha256Digest
    evaluated_at: datetime
    completeness: Completeness
    truncated: bool = False
    scope: CapabilityScope
    product: ProductProvenance
    capabilities: tuple[ProductCapability, ...] = Field(
        default_factory=tuple,
        max_length=MAX_CAPABILITIES_PER_RESPONSE,
    )
    evaluation_errors: tuple[EvaluationError, ...] = Field(
        default_factory=tuple,
        max_length=MAX_EVALUATION_ERRORS,
    )

    @field_validator("evaluated_at")
    @classmethod
    def validate_evaluated_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="evaluated_at")

    @field_validator("capabilities")
    @classmethod
    def validate_sorted_unique_capabilities(
        cls, value: tuple[ProductCapability, ...]
    ) -> tuple[ProductCapability, ...]:
        ids = [capability.id for capability in value]
        if len(set(ids)) != len(ids):
            raise ValueError("response contains duplicate capability IDs")
        if ids != sorted(ids):
            raise ValueError("response capabilities must be sorted by ID")
        return value

    @model_validator(mode="after")
    def validate_completeness(self) -> ProductCapabilitiesResponse:
        if (self.truncated or self.evaluation_errors) and (
            self.completeness is not Completeness.PARTIAL
        ):
            raise ValueError(
                "truncated/error-bearing responses must be completeness=partial"
            )
        return self


def parse_schema_version(value: str) -> tuple[int, int]:
    """Parse ``major.minor`` or raise ``ValueError``."""

    if not isinstance(value, str):
        raise ValueError("schema version must be a string")
    match = _SCHEMA_VERSION_RE.fullmatch(value)
    if not match:
        raise ValueError("schema version must use canonical major.minor form")
    return int(match.group(1)), int(match.group(2))


def schema_compatibility(value: str) -> SchemaCompatibility:
    """Classify an observed version without raising on malformed input."""

    try:
        major, minor = parse_schema_version(value)
    except ValueError:
        return SchemaCompatibility.INVALID
    current_major, current_minor = parse_schema_version(SCHEMA_VERSION)
    if major != current_major:
        return SchemaCompatibility.UNSUPPORTED_MAJOR
    if minor == current_minor:
        return SchemaCompatibility.EXACT
    return SchemaCompatibility.SAME_MAJOR


def is_supported_schema_version(value: str) -> bool:
    """Whether a bounded client may project this version onto schema 1.x."""

    return schema_compatibility(value) in {
        SchemaCompatibility.EXACT,
        SchemaCompatibility.SAME_MAJOR,
    }


def validate_capability_registry(
    definitions: Iterable[CapabilityDefinition],
) -> tuple[CapabilityDefinition, ...]:
    """Return definitions sorted by ID after cross-definition validation."""

    raw_definitions = tuple(definitions)
    if not raw_definitions:
        raise ValueError("capability registry may not be empty")
    if not all(
        isinstance(definition, CapabilityDefinition) for definition in raw_definitions
    ):
        raise TypeError("registry entries must be CapabilityDefinition objects")
    # ``model_copy(update=...)`` deliberately skips Pydantic validation. Re-run
    # the full model boundary so this registry validator remains authoritative
    # even for programmatically derived definitions.
    definitions_tuple = tuple(
        CapabilityDefinition.model_validate(
            definition.model_dump(mode="python", exclude_none=False)
        )
        for definition in raw_definitions
    )

    ids = [definition.id for definition in definitions_tuple]
    duplicate_ids = sorted({item for item in ids if ids.count(item) > 1})
    if duplicate_ids:
        raise ValueError("duplicate capability IDs: " + ", ".join(duplicate_ids))

    resolver_keys = [definition.resolver_key for definition in definitions_tuple]
    duplicate_resolvers = sorted(
        {
            resolver.value
            for resolver in resolver_keys
            if resolver_keys.count(resolver) > 1
        }
    )
    if duplicate_resolvers:
        raise ValueError(
            "duplicate capability resolver keys: " + ", ".join(duplicate_resolvers)
        )

    return tuple(sorted(definitions_tuple, key=lambda definition: definition.id))


def canonical_registry_bytes(
    definitions: Iterable[CapabilityDefinition],
) -> bytes:
    """Canonical public definition bytes used to derive registry identity."""

    validated = validate_capability_registry(definitions)
    payload = [
        definition.model_dump(mode="json", exclude_none=False)
        for definition in validated
    ]
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def capability_registry_revision(
    definitions: Iterable[CapabilityDefinition],
) -> str:
    """Return ``sha256:<hex>`` for canonical definition content."""

    digest = hashlib.sha256(canonical_registry_bytes(definitions)).hexdigest()
    return f"sha256:{digest}"


def validate_registry_guide_links(
    definitions: Iterable[CapabilityDefinition],
    guide_metadata_by_ref: Mapping[str, Mapping[str, Any]],
) -> None:
    """Validate primary guide links and reject unregistered guide capability IDs.

    The caller supplies parsed metadata so this core module remains free of
    filesystem and YAML dependencies.  Keys are relative paths such as
    ``references/datasources-email.md``.
    """

    validated = validate_capability_registry(definitions)
    registry_by_id = {definition.id: definition for definition in validated}
    errors: list[str] = []

    valid_capability_ids_by_ref: dict[str, list[str]] = {}
    for guide_ref, metadata in sorted(guide_metadata_by_ref.items()):
        capability_ids = metadata.get("capability_ids")
        if not isinstance(capability_ids, list) or not all(
            isinstance(item, str) for item in capability_ids
        ):
            errors.append(f"{guide_ref}: capability_ids must be a string list")
            continue
        if len(set(capability_ids)) != len(capability_ids):
            errors.append(f"{guide_ref}: capability_ids must be unique")
            continue
        valid_capability_ids_by_ref[guide_ref] = capability_ids
        for capability_id in capability_ids:
            if capability_id not in registry_by_id:
                errors.append(
                    f"{guide_ref}: unregistered capability ID {capability_id}"
                )

    for definition in validated:
        if definition.guide_ref not in guide_metadata_by_ref:
            errors.append(
                f"{definition.id}: missing primary guide {definition.guide_ref}"
            )
            continue
        capability_ids = valid_capability_ids_by_ref.get(definition.guide_ref)
        if capability_ids is not None and definition.id not in capability_ids:
            errors.append(
                f"{definition.id}: absent from {definition.guide_ref} capability_ids"
            )

    if errors:
        raise ValueError("invalid capability/guide linkage: " + "; ".join(errors))


def validate_capability_against_definition(
    capability: ProductCapability,
    definition: CapabilityDefinition,
) -> None:
    """Validate one result's static metadata and public qualifier allowlist."""

    if capability.id != definition.id:
        raise ValueError("capability result ID does not match definition")
    if capability.visibility is not definition.visibility:
        raise ValueError("capability visibility does not match definition")
    if capability.help_topic_id != definition.help_topic_id:
        raise ValueError("capability help topic does not match definition")
    if capability.open_action_id != definition.open_action_id:
        raise ValueError("capability open action does not match definition")
    if capability.visual_id != definition.visual_id:
        raise ValueError("capability visual ID does not match definition")

    emitted_keys = {
        qualifier.key
        for layer in (
            capability.build,
            capability.deployment,
            capability.user,
            capability.session,
        )
        for qualifier in layer.qualifiers
    }
    unexpected = emitted_keys - set(definition.public_qualifier_keys)
    if unexpected:
        raise ValueError(
            "capability emitted undeclared qualifier keys: "
            + ", ".join(sorted(unexpected))
        )


def validate_response_against_registry(
    response: ProductCapabilitiesResponse,
    definitions: Iterable[CapabilityDefinition],
) -> None:
    """Server-side validation that a response belongs to the supplied registry."""

    validated = validate_capability_registry(definitions)
    expected_revision = capability_registry_revision(validated)
    if response.registry_revision != expected_revision:
        raise ValueError("response registry_revision does not match definitions")
    registry_by_id = {definition.id: definition for definition in validated}
    for capability in response.capabilities:
        definition = registry_by_id.get(capability.id)
        if definition is None:
            raise ValueError(
                f"response contains unregistered capability {capability.id}"
            )
        validate_capability_against_definition(capability, definition)


def _unknown_layer(
    layer: LayerName,
    *,
    source_component: ProductComponent,
    observed_at: datetime,
) -> LayerEvaluation:
    common = {
        "source_component": source_component,
        "freshness": Freshness.UNKNOWN,
        "observed_at": observed_at,
        "qualifiers": (),
    }
    if layer is LayerName.BUILD:
        return BuildEvaluation(
            state=BuildState.UNKNOWN,
            reason_code=ReasonCode.BUILD_OBSERVATION_UNAVAILABLE,
            **common,
        )
    if layer is LayerName.DEPLOYMENT:
        return DeploymentEvaluation(
            state=DeploymentState.UNKNOWN,
            reason_code=ReasonCode.DEPLOYMENT_OBSERVATION_UNAVAILABLE,
            **common,
        )
    if layer is LayerName.USER:
        return UserEvaluation(
            state=UserState.UNKNOWN,
            reason_code=ReasonCode.USER_OBSERVATION_UNAVAILABLE,
            **common,
        )
    return SessionEvaluation(
        state=SessionState.UNKNOWN,
        reason_code=ReasonCode.SESSION_OBSERVATION_UNAVAILABLE,
        **common,
    )


_LAYER_MODEL_TYPES: Mapping[LayerName, type[_LayerEvaluation]] = MappingProxyType(
    {
        LayerName.BUILD: BuildEvaluation,
        LayerName.DEPLOYMENT: DeploymentEvaluation,
        LayerName.USER: UserEvaluation,
        LayerName.SESSION: SessionEvaluation,
    }
)


def evaluate_layer_safely(
    layer: LayerName,
    resolver: Callable[[], LayerEvaluation],
    *,
    capability_id: CapabilityId,
    source_component: ProductComponent,
    observed_at: datetime,
) -> tuple[LayerEvaluation, EvaluationError | None]:
    """Run one synchronous pure resolver and map any exception to unknown.

    Async/database orchestration is intentionally outside this module.  M2b
    can call this helper around each already-bounded pure adapter after it has
    gathered its inputs.
    """

    try:
        result = resolver()
        expected_type = _LAYER_MODEL_TYPES[layer]
        if not isinstance(result, expected_type):
            raise TypeError(
                f"{layer.value} resolver returned {type(result).__name__}, "
                f"expected {expected_type.__name__}"
            )
        return result, None
    except Exception:
        return (
            _unknown_layer(
                layer,
                source_component=source_component,
                observed_at=observed_at,
            ),
            EvaluationError(
                code=EvaluationErrorCode.RESOLVER_ERROR,
                capability_id=capability_id,
                layer=layer,
                source_component=source_component,
                retryable=True,
            ),
        )


def derive_agent_action(
    *,
    build: BuildEvaluation,
    deployment: DeploymentEvaluation,
    user: UserEvaluation,
    session: SessionEvaluation,
    has_execute_tool: bool,
    has_proposal_tool: bool,
    has_guide: bool,
) -> AgentAction:
    """Conservatively derive model actionability from proven layer state."""

    has_unknown = (
        build.state is BuildState.UNKNOWN
        or deployment.state is DeploymentState.UNKNOWN
        or user.state in {UserState.UNKNOWN, UserState.NO_OPINION}
        or session.state is SessionState.UNKNOWN
    )
    if session.state is SessionState.NOT_APPLICABLE:
        return AgentAction.UNKNOWN
    if has_unknown:
        return AgentAction.CAN_GUIDE if has_guide else AgentAction.UNKNOWN

    upper_layers_allow = (
        build.state is BuildState.SUPPORTED
        and deployment.state is DeploymentState.ENABLED
        and user.state is UserState.ALLOWED
    )
    if not upper_layers_allow:
        return AgentAction.CAN_GUIDE if has_guide else AgentAction.UNAVAILABLE

    if session.state is SessionState.READY:
        if has_execute_tool:
            return AgentAction.CAN_EXECUTE
        if has_proposal_tool:
            return AgentAction.CAN_PROPOSE

    return AgentAction.CAN_GUIDE if has_guide else AgentAction.UNAVAILABLE


_COMPONENTS = CapabilityComponents(
    authority=ProductComponent.ORCHESTRATOR,
    execution=ProductComponent.AGENT,
    presentation=ProductComponent.COCKPIT,
    guidance=ProductComponent.GUIDE,
)


def _definition(
    capability_id: str,
    *,
    topics: tuple[str, ...],
    title_key: str,
    summary: str,
    help_topic_id: str,
    resolver_key: CapabilityResolverKey,
    qualifier_keys: tuple[str, ...] = (),
) -> CapabilityDefinition:
    return CapabilityDefinition(
        id=capability_id,
        topics=topics,
        components=_COMPONENTS,
        visibility=CapabilityVisibility.PUBLIC,
        title_key=title_key,
        summary=summary,
        help_topic_id=help_topic_id,
        guide_ref=f"references/{help_topic_id}.md",
        resolver_key=resolver_key,
        public_qualifier_keys=qualifier_keys,
    )


CAPABILITY_DEFINITIONS = validate_capability_registry(
    (
        _definition(
            "automations.manage",
            topics=("automations", "jobs"),
            title_key="productCapabilities.automationsManage",
            summary="Create and inspect scheduled worker-job automations.",
            help_topic_id="automations",
            resolver_key=CapabilityResolverKey.AUTOMATIONS_MANAGE,
        ),
        _definition(
            "canvas.browser",
            topics=("browser", "canvas"),
            title_key="productCapabilities.canvasBrowser",
            summary="Share and collaboratively control an attested workspace browser.",
            help_topic_id="canvas-and-browser",
            resolver_key=CapabilityResolverKey.CANVAS_BROWSER,
        ),
        _definition(
            "canvas.files",
            topics=("canvas", "files"),
            title_key="productCapabilities.canvasFiles",
            summary="Present supported workspace files and apps in Canvas.",
            help_topic_id="canvas-and-browser",
            resolver_key=CapabilityResolverKey.CANVAS_FILES,
        ),
        _definition(
            "datasources.email",
            topics=("datasources", "email"),
            title_key="productCapabilities.datasourcesEmail",
            summary="Attach a scoped IMAP and SMTP mailbox to agent work.",
            help_topic_id="datasources-email",
            resolver_key=CapabilityResolverKey.DATASOURCE_EMAIL,
            qualifier_keys=("email.access_tier",),
        ),
        _definition(
            "datasources.email.send",
            topics=("datasources", "email"),
            title_key="productCapabilities.datasourcesEmailSend",
            summary="Send email when the attached mailbox tier and approval path allow it.",
            help_topic_id="datasources-email",
            resolver_key=CapabilityResolverKey.DATASOURCE_EMAIL_SEND,
            qualifier_keys=("email.access_tier",),
        ),
        _definition(
            "datasources.okf",
            topics=("datasources", "knowledge", "okf"),
            title_key="productCapabilities.datasourcesOkf",
            summary="Attach a read-only OKF repository as searchable project knowledge.",
            help_topic_id="datasources-okf",
            resolver_key=CapabilityResolverKey.DATASOURCE_OKF,
        ),
        _definition(
            "experts.manage",
            topics=("experts",),
            title_key="productCapabilities.expertsManage",
            summary="Create and maintain reusable user-defined expert profiles.",
            help_topic_id="experts",
            resolver_key=CapabilityResolverKey.EXPERTS_MANAGE,
        ),
        _definition(
            "experts.select",
            topics=("experts",),
            title_key="productCapabilities.expertsSelect",
            summary="Choose a compatible expert for a session or worker job.",
            help_topic_id="experts",
            resolver_key=CapabilityResolverKey.EXPERTS_SELECT,
        ),
        _definition(
            "jobs.create",
            topics=("jobs",),
            title_key="productCapabilities.jobsCreate",
            summary="Create autonomous worker jobs in the SRW fleet.",
            help_topic_id="jobs",
            resolver_key=CapabilityResolverKey.JOBS_CREATE,
        ),
        _definition(
            "jobs.review",
            topics=("jobs",),
            title_key="productCapabilities.jobsReview",
            summary="Inspect, approve, resume, pause, or cancel accessible worker jobs.",
            help_topic_id="jobs",
            resolver_key=CapabilityResolverKey.JOBS_REVIEW,
        ),
        _definition(
            "memory.recall",
            topics=("memory",),
            title_key="productCapabilities.memoryRecall",
            summary="Recall durable memories when the active runtime has memory enabled.",
            help_topic_id="memory-and-knowledge",
            resolver_key=CapabilityResolverKey.MEMORY_RECALL,
        ),
        _definition(
            "projects.knowledge",
            topics=("knowledge", "projects"),
            title_key="productCapabilities.projectsKnowledge",
            summary="Search and maintain knowledge within the active project scope.",
            help_topic_id="memory-and-knowledge",
            resolver_key=CapabilityResolverKey.PROJECT_KNOWLEDGE,
        ),
        _definition(
            "projects.loops",
            topics=("loops", "projects"),
            title_key="productCapabilities.projectsLoops",
            summary="Inspect and operate bounded standard or campaign project loops.",
            help_topic_id="project-loops",
            resolver_key=CapabilityResolverKey.PROJECT_LOOPS,
        ),
        _definition(
            "projects.manage",
            topics=("projects",),
            title_key="productCapabilities.projectsManage",
            summary="Organize work, experts, connectors, repositories, and jobs in projects.",
            help_topic_id="projects-and-loops",
            resolver_key=CapabilityResolverKey.PROJECTS_MANAGE,
        ),
        _definition(
            "sessions.delegate",
            topics=("delegation", "jobs", "sessions"),
            title_key="productCapabilities.sessionsDelegate",
            summary="Delegate bounded work from a live session to worker jobs or built-in subagents.",
            help_topic_id="fleet-and-delegation",
            resolver_key=CapabilityResolverKey.SESSION_DELEGATE,
        ),
        _definition(
            "sessions.permission-mode",
            topics=("permissions", "sessions"),
            title_key="productCapabilities.sessionsPermissionMode",
            summary="Choose a session permission mode within the caller's effective ceiling.",
            help_topic_id="permissions-and-availability",
            resolver_key=CapabilityResolverKey.SESSION_PERMISSION_MODE,
            qualifier_keys=("permission_mode.ceiling",),
        ),
        _definition(
            "sessions.protected-cloud",
            topics=("cloud", "sessions"),
            title_key="productCapabilities.sessionsProtectedCloud",
            summary="Stage eligible cloud changes for explicit review before applying them.",
            help_topic_id="protected-cloud",
            resolver_key=CapabilityResolverKey.SESSION_PROTECTED_CLOUD,
        ),
        _definition(
            "workspaces.select",
            topics=("sessions", "workspaces"),
            title_key="productCapabilities.workspacesSelect",
            summary="Select an available isolated workspace tier for agent execution.",
            help_topic_id="permissions-and-availability",
            resolver_key=CapabilityResolverKey.WORKSPACE_SELECT,
            qualifier_keys=(
                "workspace.backend",
                "workspace.supports_shell",
            ),
        ),
    )
)

CAPABILITY_REGISTRY: Mapping[str, CapabilityDefinition] = MappingProxyType(
    {definition.id: definition for definition in CAPABILITY_DEFINITIONS}
)
INITIAL_CAPABILITY_IDS = tuple(CAPABILITY_REGISTRY)
REGISTRY_REVISION = capability_registry_revision(CAPABILITY_DEFINITIONS)


__all__ = [
    "AgentAction",
    "BuildEvaluation",
    "BuildState",
    "CAPABILITY_DEFINITIONS",
    "CAPABILITY_REGISTRY",
    "CapabilityComponents",
    "CapabilityDefinition",
    "CapabilityQualifier",
    "CapabilityResolverKey",
    "CapabilityScope",
    "CapabilityVisibility",
    "Completeness",
    "ComponentProvenance",
    "DeploymentEvaluation",
    "DeploymentState",
    "EvaluationError",
    "EvaluationErrorCode",
    "Freshness",
    "INITIAL_CAPABILITY_IDS",
    "LayerEvaluation",
    "LayerName",
    "MAX_CAPABILITIES_PER_RESPONSE",
    "MAX_EVALUATION_ERRORS",
    "MAX_LAYER_QUALIFIERS",
    "MAX_PUBLIC_QUALIFIER_KEYS",
    "MAX_QUALIFIER_LIST_ITEMS",
    "ProductCapabilitiesResponse",
    "ProductCapability",
    "ProductComponent",
    "ProductProvenance",
    "ProvenanceStatus",
    "PUBLIC_QUALIFIER_KEYS",
    "REASON_CODE_COMPATIBILITY",
    "REGISTRY_REVISION",
    "ReasonCode",
    "SCHEMA_MAJOR",
    "SCHEMA_VERSION",
    "SchemaCompatibility",
    "ScopeKind",
    "SessionEvaluation",
    "SessionState",
    "UserEvaluation",
    "UserState",
    "canonical_registry_bytes",
    "capability_registry_revision",
    "derive_agent_action",
    "evaluate_layer_safely",
    "is_supported_schema_version",
    "parse_schema_version",
    "schema_compatibility",
    "validate_capability_against_definition",
    "validate_capability_registry",
    "validate_reason_code",
    "validate_registry_guide_links",
    "validate_response_against_registry",
]
