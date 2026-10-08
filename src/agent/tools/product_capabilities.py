"""Bounded live product-capability observation for persistent sessions.

The orchestrator owns build, deployment, caller, and primary-project policy
facts. This tool binds the current ``ToolContext`` user/thread to that endpoint
and overlays only the immutable, redacted runtime facts published by the
persistent session after final tool instantiation.

The result is advisory. It is never passed to an operation as authorization;
the real operation remains responsible for current policy enforcement.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any
from uuid import UUID

import httpx
from langchain_core.tools import tool
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from shared.runtime.core.product_capabilities import (
    AgentAction,
    CAPABILITY_REGISTRY,
    CapabilityDefinition,
    CapabilityQualifier,
    CapabilityResolverKey,
    Completeness,
    EvaluationError,
    EvaluationErrorCode,
    Freshness,
    LayerName,
    MAX_EVALUATION_ERRORS,
    ProductCapabilitiesResponse,
    ProductCapability,
    ProductComponent,
    ProductProvenance,
    ReasonCode,
    SCHEMA_VERSION,
    SchemaCompatibility,
    ScopeKind,
    SessionEvaluation,
    SessionState,
    TopicId,
    derive_agent_action,
    schema_compatibility,
    validate_capability_against_definition,
)
from shared.runtime.core.runtime_provenance import merge_product_provenance

from agent.tools.context import (
    EMAIL_TOOL_CATEGORY,
    SessionRuntimeFacts,
    ToolContext,
)

from shared.tool_catalog.definitions import (
    PRODUCT_CAPABILITY_TOOLS_METADATA as PRODUCT_CAPABILITY_TOOLS_METADATA,
)

from shared.tool_catalog.names import (
    PRODUCT_CAPABILITIES_TOOL_NAME as PRODUCT_CAPABILITIES_TOOL_NAME,
)

logger = logging.getLogger(__name__)

PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV = "PRODUCT_CAPABILITIES_TOOL_ENABLED"
MAX_TOOL_CAPABILITY_IDS = 12
MAX_SERVER_RESPONSE_BYTES = 64 * 1024
MAX_TOOL_OUTPUT_BYTES = 32 * 1024
CAPABILITY_REQUEST_TIMEOUT_SECONDS = 5.0
_DEFAULT_RESULT_LIMIT = 20
_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})

_CapabilityId = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=120,
        pattern=r"^[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+$",
    ),
]


class CapabilityToolStatus(str, Enum):
    READY = "ready"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class CapabilityToolErrorCode(str, Enum):
    INVALID_REQUEST = "invalid_request"
    SCOPE_UNAVAILABLE = "scope_unavailable"
    ENDPOINT_TIMEOUT = "endpoint_timeout"
    ENDPOINT_UNAVAILABLE = "endpoint_unavailable"
    RESPONSE_TOO_LARGE = "response_too_large"
    INVALID_RESPONSE = "invalid_response"
    UNSUPPORTED_SCHEMA = "unsupported_schema"
    RUNTIME_FACTS_UNAVAILABLE = "runtime_facts_unavailable"
    PROJECTION_INCOMPLETE = "projection_incomplete"
    OUTPUT_TRUNCATED = "output_truncated"


class CapabilityToolRequest(BaseModel):
    """Closed model-facing request; identity and scope are deliberately absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    topic: TopicId | None = None
    capability_ids: tuple[_CapabilityId, ...] = Field(
        default_factory=tuple,
        max_length=MAX_TOOL_CAPABILITY_IDS,
    )

    @field_validator("topic")
    @classmethod
    def validate_known_topic(cls, value: str | None) -> str | None:
        if value is None:
            return None
        known_topics = {
            topic
            for definition in CAPABILITY_REGISTRY.values()
            for topic in definition.topics
        }
        if value not in known_topics:
            raise ValueError("unknown capability topic")
        return value

    @field_validator("capability_ids", mode="before")
    @classmethod
    def validate_raw_count(cls, value: Any) -> Any:
        if value is None:
            return ()
        if isinstance(value, (list, tuple)) and len(value) > MAX_TOOL_CAPABILITY_IDS:
            raise ValueError(
                f"at most {MAX_TOOL_CAPABILITY_IDS} capability IDs are allowed"
            )
        return value

    @field_validator("capability_ids")
    @classmethod
    def canonicalize_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted(set(value)))


class ProductCapabilitiesToolOutput(BaseModel):
    """Validated JSON envelope returned to the model."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: CapabilityToolStatus
    schema_compatibility: SchemaCompatibility | None = None
    error_code: CapabilityToolErrorCode | None = None
    summary: str = Field(min_length=1, max_length=6_000)
    response: ProductCapabilitiesResponse | None = None

    @model_validator(mode="after")
    def validate_status_shape(self) -> ProductCapabilitiesToolOutput:
        if self.status is CapabilityToolStatus.UNAVAILABLE:
            if self.response is not None or self.error_code is None:
                raise ValueError("unavailable output requires an error and no response")
        elif self.response is None:
            raise ValueError("ready/partial output requires a capability response")
        if (
            self.status is CapabilityToolStatus.READY
            and self.response is not None
            and self.response.completeness is not Completeness.COMPLETE
        ):
            raise ValueError("ready output requires a complete response")
        return self


def get_product_capabilities_metadata() -> dict[str, dict[str, Any]]:
    return PRODUCT_CAPABILITY_TOOLS_METADATA


def product_capabilities_tool_enabled(
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Return the temporary operator-owned M2c canary gate."""

    source = os.environ if environment is None else environment
    return (
        source.get(PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV, "").strip().lower()
        in _TRUTHY_ENV_VALUES
    )


class _CapabilityToolFailure(Exception):
    def __init__(
        self,
        code: CapabilityToolErrorCode,
        *,
        compatibility: SchemaCompatibility | None = None,
    ) -> None:
        self.code = code
        self.compatibility = compatibility
        super().__init__(code.value)


CapabilityFetcher = Callable[
    [ToolContext, CapabilityToolRequest],
    Awaitable[bytes],
]


async def _fetch_server_payload(
    context: ToolContext,
    request: CapabilityToolRequest,
) -> bytes:
    """Fetch the bound endpoint without accepting model-controlled identity."""

    try:
        user_id = str(UUID(str(context.user_id)))
        thread_id = str(UUID(str(context.thread_id)))
    except (TypeError, ValueError):
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.SCOPE_UNAVAILABLE
        ) from None

    headers = {"Accept": "application/json", "X-MCP-User-Id": user_id}
    internal_key = os.getenv("MCP_INTERNAL_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key

    params: list[tuple[str, str]] = [
        ("thread_id", thread_id),
        ("limit", str(_DEFAULT_RESULT_LIMIT)),
    ]
    if request.topic is not None:
        params.append(("topic", request.topic))
    params.extend(("capability_id", item) for item in request.capability_ids)

    base_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085").rstrip("/")
    timeout = httpx.Timeout(CAPABILITY_REQUEST_TIMEOUT_SECONDS)
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            headers=headers,
            follow_redirects=False,
        ) as client:
            async with client.stream(
                "GET",
                f"{base_url}/api/users/me/product-capabilities",
                params=params,
            ) as response:
                if response.status_code != 200:
                    raise _CapabilityToolFailure(
                        CapabilityToolErrorCode.ENDPOINT_UNAVAILABLE
                    )
                content_length = response.headers.get("content-length")
                if content_length is not None:
                    try:
                        if int(content_length) > MAX_SERVER_RESPONSE_BYTES:
                            raise _CapabilityToolFailure(
                                CapabilityToolErrorCode.RESPONSE_TOO_LARGE
                            )
                    except ValueError:
                        pass

                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_SERVER_RESPONSE_BYTES:
                        raise _CapabilityToolFailure(
                            CapabilityToolErrorCode.RESPONSE_TOO_LARGE
                        )
                    chunks.append(chunk)
                return b"".join(chunks)
    except _CapabilityToolFailure:
        raise
    except httpx.TimeoutException:
        raise _CapabilityToolFailure(CapabilityToolErrorCode.ENDPOINT_TIMEOUT) from None
    except httpx.HTTPError:
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.ENDPOINT_UNAVAILABLE
        ) from None


def _safe_failure_output(
    code: CapabilityToolErrorCode,
    *,
    compatibility: SchemaCompatibility | None = None,
) -> ProductCapabilitiesToolOutput:
    summaries = {
        CapabilityToolErrorCode.INVALID_REQUEST: (
            "The capability request was invalid. Use one exact topic ID and no "
            f"more than {MAX_TOOL_CAPABILITY_IDS} exact capability IDs."
        ),
        CapabilityToolErrorCode.SCOPE_UNAVAILABLE: (
            "Current capability state is unavailable because this tool is not "
            "bound to a valid persistent-session user and thread."
        ),
        CapabilityToolErrorCode.ENDPOINT_TIMEOUT: (
            "Current capability state could not be checked because the "
            "orchestrator request timed out. Treat availability as unknown."
        ),
        CapabilityToolErrorCode.ENDPOINT_UNAVAILABLE: (
            "Current capability state could not be checked because the "
            "orchestrator endpoint is unavailable. Treat availability as unknown."
        ),
        CapabilityToolErrorCode.RESPONSE_TOO_LARGE: (
            "Current capability state could not be checked because the server "
            "response exceeded the safe size limit. Treat availability as unknown."
        ),
        CapabilityToolErrorCode.INVALID_RESPONSE: (
            "Current capability state could not be checked because the server "
            "response failed validation. Treat availability as unknown."
        ),
        CapabilityToolErrorCode.UNSUPPORTED_SCHEMA: (
            "Current capability state uses an unsupported contract version. "
            "Treat availability as unknown until the agent is upgraded."
        ),
        CapabilityToolErrorCode.RUNTIME_FACTS_UNAVAILABLE: (
            "Live session facts are unavailable. Server capability layers may "
            "still be shown, but current tools and actionability are unknown."
        ),
        CapabilityToolErrorCode.PROJECTION_INCOMPLETE: (
            "The server response contained capabilities this agent does not "
            "know; only known capabilities are shown."
        ),
        CapabilityToolErrorCode.OUTPUT_TRUNCATED: (
            "The capability result was truncated to the safe tool-output limit."
        ),
    }
    return ProductCapabilitiesToolOutput(
        status=CapabilityToolStatus.UNAVAILABLE,
        schema_compatibility=compatibility,
        error_code=code,
        summary=summaries[code],
    )


def _parse_server_response(
    payload_bytes: bytes,
    *,
    context: ToolContext,
    request: CapabilityToolRequest,
) -> tuple[ProductCapabilitiesResponse, SchemaCompatibility, bool]:
    if len(payload_bytes) > MAX_SERVER_RESPONSE_BYTES:
        raise _CapabilityToolFailure(CapabilityToolErrorCode.RESPONSE_TOO_LARGE)
    try:
        raw = json.loads(payload_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _CapabilityToolFailure(CapabilityToolErrorCode.INVALID_RESPONSE) from None
    if not isinstance(raw, dict):
        raise _CapabilityToolFailure(CapabilityToolErrorCode.INVALID_RESPONSE)

    compatibility = schema_compatibility(raw.get("schema_version"))
    if compatibility is SchemaCompatibility.UNSUPPORTED_MAJOR:
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.UNSUPPORTED_SCHEMA,
            compatibility=compatibility,
        )
    if compatibility is SchemaCompatibility.INVALID:
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.INVALID_RESPONSE,
            compatibility=compatibility,
        )

    projected = dict(raw)
    if compatibility is SchemaCompatibility.SAME_MAJOR:
        projected["schema_version"] = SCHEMA_VERSION
        product = projected.get("product")
        if isinstance(product, dict) and isinstance(product.get("components"), dict):
            known_components = {item.value for item in ProductComponent}
            projected["product"] = {
                **product,
                "components": {
                    key: value
                    for key, value in product["components"].items()
                    if key in known_components
                },
            }

    try:
        response = ProductCapabilitiesResponse.model_validate(
            projected,
            extra=(
                "ignore"
                if compatibility is SchemaCompatibility.SAME_MAJOR
                else "forbid"
            ),
        )
        expected_thread = UUID(str(context.thread_id))
    except (ValidationError, TypeError, ValueError):
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.INVALID_RESPONSE,
            compatibility=compatibility,
        ) from None

    if (
        response.scope.kind is not ScopeKind.THREAD
        or response.scope.thread_id != expected_thread
    ):
        raise _CapabilityToolFailure(
            CapabilityToolErrorCode.INVALID_RESPONSE,
            compatibility=compatibility,
        )

    requested_ids = set(request.capability_ids)
    known: list[ProductCapability] = []
    projection_incomplete = False
    for capability in response.capabilities:
        definition = CAPABILITY_REGISTRY.get(capability.id)
        if definition is None:
            projection_incomplete = True
            continue
        if requested_ids and capability.id not in requested_ids:
            raise _CapabilityToolFailure(
                CapabilityToolErrorCode.INVALID_RESPONSE,
                compatibility=compatibility,
            )
        if request.topic is not None and request.topic not in definition.topics:
            raise _CapabilityToolFailure(
                CapabilityToolErrorCode.INVALID_RESPONSE,
                compatibility=compatibility,
            )
        try:
            validate_capability_against_definition(capability, definition)
        except ValueError:
            raise _CapabilityToolFailure(
                CapabilityToolErrorCode.INVALID_RESPONSE,
                compatibility=compatibility,
            ) from None
        if (
            capability.build.source_component is not ProductComponent.REGISTRY
            or capability.deployment.source_component
            is not ProductComponent.ORCHESTRATOR
            or capability.user.source_component is not ProductComponent.ORCHESTRATOR
            or capability.session.source_component is not ProductComponent.ORCHESTRATOR
            or capability.agent_action is not AgentAction.UNKNOWN
        ):
            raise _CapabilityToolFailure(
                CapabilityToolErrorCode.INVALID_RESPONSE,
                compatibility=compatibility,
            )
        known.append(capability)

    if projection_incomplete:
        response = _replace_response(
            response,
            capabilities=known,
            truncated=True,
            completeness=Completeness.PARTIAL,
        )
    return response, compatibility, projection_incomplete


@dataclass(frozen=True, slots=True)
class _ToolRule:
    readiness: frozenset[str]
    execute: frozenset[str] = frozenset()
    proposal: frozenset[str] = frozenset()


_TOOL_RULES: Mapping[CapabilityResolverKey, _ToolRule] = {
    CapabilityResolverKey.AUTOMATIONS_MANAGE: _ToolRule(
        readiness=frozenset(
            {
                "get_automation",
                "list_automations",
                "propose_automation",
                "set_automation_bundle",
            }
        ),
        execute=frozenset({"set_automation_bundle"}),
        proposal=frozenset({"propose_automation"}),
    ),
    CapabilityResolverKey.EXPERTS_MANAGE: _ToolRule(
        readiness=frozenset({"get_expert", "list_experts", "set_expert_bundle"}),
        execute=frozenset({"set_expert_bundle"}),
    ),
    CapabilityResolverKey.EXPERTS_SELECT: _ToolRule(
        readiness=frozenset({"get_expert", "list_experts"}),
    ),
    CapabilityResolverKey.JOBS_CREATE: _ToolRule(
        readiness=frozenset({"create_job"}),
        execute=frozenset({"create_job"}),
    ),
    CapabilityResolverKey.JOBS_REVIEW: _ToolRule(
        readiness=frozenset(
            {
                "approve_job",
                "cancel_job",
                "get_job",
                "pause_job",
                "resume_job_with_feedback",
            }
        ),
        execute=frozenset(
            {
                "approve_job",
                "cancel_job",
                "get_job",
                "pause_job",
                "resume_job_with_feedback",
            }
        ),
    ),
    CapabilityResolverKey.PROJECT_LOOPS: _ToolRule(
        readiness=frozenset(
            {"explain_project_loop", "get_project_loop", "list_project_loop_jobs"}
        ),
        execute=frozenset(
            {"explain_project_loop", "get_project_loop", "list_project_loop_jobs"}
        ),
    ),
    CapabilityResolverKey.PROJECTS_MANAGE: _ToolRule(
        readiness=frozenset(
            {
                "get_current_project",
                "list_project_jobs",
                "list_project_repositories",
            }
        ),
    ),
    CapabilityResolverKey.SESSION_DELEGATE: _ToolRule(
        readiness=frozenset({"create_job", "delegate_agent"}),
        execute=frozenset({"create_job", "delegate_agent"}),
    ),
    CapabilityResolverKey.WORKSPACE_SELECT: _ToolRule(
        readiness=frozenset({"request_workspace_upgrade"}),
        proposal=frozenset({"request_workspace_upgrade"}),
    ),
}


def _session_result(
    state: SessionState,
    reason: ReasonCode,
    facts: SessionRuntimeFacts,
    *,
    qualifiers: tuple[CapabilityQualifier, ...] = (),
    freshness: Freshness = Freshness.FRESH,
) -> SessionEvaluation:
    return SessionEvaluation(
        state=state,
        reason_code=reason,
        source_component=ProductComponent.AGENT,
        freshness=freshness,
        observed_at=facts.observed_at,
        qualifiers=qualifiers,
    )


def _tool_based_session(
    rule: _ToolRule,
    facts: SessionRuntimeFacts,
) -> SessionEvaluation:
    if rule.readiness.intersection(facts.loaded_tool_names):
        return _session_result(
            SessionState.READY,
            ReasonCode.TOOL_LOADED,
            facts,
        )
    return _session_result(
        SessionState.NOT_READY,
        ReasonCode.TOOL_NOT_LOADED,
        facts,
    )


def _email_session(
    definition: CapabilityDefinition,
    facts: SessionRuntimeFacts,
) -> SessionEvaluation:
    if EMAIL_TOOL_CATEGORY not in facts.attached_tool_categories:
        return _session_result(
            SessionState.NEEDS_ATTACHMENT,
            ReasonCode.DATASOURCE_NOT_ATTACHED,
            facts,
        )
    qualifiers = (
        CapabilityQualifier(
            key="email.access_tier",
            value=facts.email_access_tier,
        ),
    )
    if facts.email_connection_failed:
        return _session_result(
            SessionState.DEGRADED,
            ReasonCode.DATASOURCE_CONNECTION_DEGRADED,
            facts,
            qualifiers=qualifiers,
        )

    loaded = set(facts.loaded_tool_names)
    if definition.resolver_key is CapabilityResolverKey.DATASOURCE_EMAIL:
        ready = "email_read" in loaded
    else:
        ready = (facts.email_access_tier == "send" and "email_send" in loaded) or (
            facts.email_access_tier in {"draft", "send"} and "email_draft" in loaded
        )
    if ready:
        return _session_result(
            SessionState.READY,
            ReasonCode.DATASOURCE_ATTACHED,
            facts,
            qualifiers=qualifiers,
        )
    return _session_result(
        SessionState.NOT_READY,
        ReasonCode.TOOL_NOT_LOADED,
        facts,
        qualifiers=qualifiers,
    )


def _resolve_live_session(
    definition: CapabilityDefinition,
    facts: SessionRuntimeFacts,
) -> SessionEvaluation | None:
    key = definition.resolver_key
    loaded = set(facts.loaded_tool_names)

    if key in {
        CapabilityResolverKey.DATASOURCE_EMAIL,
        CapabilityResolverKey.DATASOURCE_EMAIL_SEND,
    }:
        return _email_session(definition, facts)

    if key is CapabilityResolverKey.DATASOURCE_OKF:
        if not facts.knowledge_index_attached:
            return _session_result(
                SessionState.NEEDS_ATTACHMENT,
                ReasonCode.DATASOURCE_NOT_ATTACHED,
                facts,
            )
        if (
            facts.knowledge_binding_available
            and facts.knowledge_store_available
            and {"kb_read", "kb_search", "kb_grep"}.intersection(loaded)
        ):
            return _session_result(
                SessionState.READY,
                ReasonCode.KNOWLEDGE_READY,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.KNOWLEDGE_UNAVAILABLE,
            facts,
        )

    if key is CapabilityResolverKey.PROJECT_KNOWLEDGE:
        if (
            facts.knowledge_binding_available
            and facts.knowledge_store_available
            and {"kb_read", "kb_search", "kb_grep"}.intersection(loaded)
        ):
            return _session_result(
                SessionState.READY,
                ReasonCode.KNOWLEDGE_READY,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.KNOWLEDGE_UNAVAILABLE,
            facts,
        )

    if key is CapabilityResolverKey.MEMORY_RECALL:
        if facts.memory_available:
            return _session_result(
                SessionState.READY,
                ReasonCode.KNOWLEDGE_READY,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.KNOWLEDGE_UNAVAILABLE,
            facts,
        )

    if key is CapabilityResolverKey.CANVAS_FILES:
        if not facts.backend_supports_file_tools:
            return _session_result(
                SessionState.NEEDS_UPGRADE,
                ReasonCode.WORKSPACE_REQUIRED,
                facts,
            )
        if not facts.backend_supports_canvas_presentation:
            return _session_result(
                SessionState.NOT_READY,
                ReasonCode.WORKSPACE_NOT_READY,
                facts,
            )
        if "set_canvas" in loaded:
            return _session_result(
                SessionState.READY,
                ReasonCode.TOOL_LOADED,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.TOOL_NOT_LOADED,
            facts,
        )

    if key is CapabilityResolverKey.CANVAS_BROWSER:
        if not facts.backend_supports_shared_browser:
            if not facts.backend_supports_shell:
                return _session_result(
                    SessionState.NEEDS_UPGRADE,
                    ReasonCode.WORKSPACE_REQUIRED,
                    facts,
                )
            return _session_result(
                SessionState.NOT_READY,
                ReasonCode.WORKSPACE_NOT_READY,
                facts,
            )
        if "browser_navigate" in loaded:
            return _session_result(
                SessionState.READY,
                ReasonCode.TOOL_LOADED,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.TOOL_NOT_LOADED,
            facts,
        )

    if key is CapabilityResolverKey.WORKSPACE_SELECT:
        if facts.backend_id is None:
            raise RuntimeError("active backend is not classifiable")
        qualifiers = (
            CapabilityQualifier(
                key="workspace.backend",
                value=facts.backend_id,
            ),
            CapabilityQualifier(
                key="workspace.supports_shell",
                value=facts.backend_supports_shell,
            ),
        )
        return _session_result(
            SessionState.READY,
            ReasonCode.WORKSPACE_READY,
            facts,
            qualifiers=qualifiers,
        )

    if key is CapabilityResolverKey.SESSION_PROTECTED_CLOUD:
        if facts.cloud_mount_active and facts.protected_cloud_active:
            return _session_result(
                SessionState.READY,
                ReasonCode.CLOUD_ATTACHED,
                facts,
            )
        return _session_result(
            SessionState.NOT_READY,
            ReasonCode.CLOUD_NOT_ATTACHED,
            facts,
        )

    rule = _TOOL_RULES.get(key)
    if rule is not None:
        return _tool_based_session(rule, facts)
    return None


def _action_flags(
    definition: CapabilityDefinition,
    facts: SessionRuntimeFacts,
) -> tuple[bool, bool]:
    key = definition.resolver_key
    loaded = set(facts.loaded_tool_names)

    if key is CapabilityResolverKey.DATASOURCE_EMAIL:
        return (
            EMAIL_TOOL_CATEGORY in facts.attached_tool_categories
            and not facts.email_connection_failed
            and "email_read" in loaded,
            False,
        )
    if key is CapabilityResolverKey.DATASOURCE_EMAIL_SEND:
        return (
            facts.email_access_tier == "send"
            and not facts.email_connection_failed
            and facts.email_direct_send_enabled
            and "email_send" in loaded,
            facts.email_access_tier in {"draft", "send"}
            and not facts.email_connection_failed
            and "email_draft" in loaded,
        )
    if key in {
        CapabilityResolverKey.DATASOURCE_OKF,
        CapabilityResolverKey.PROJECT_KNOWLEDGE,
    }:
        return (
            facts.knowledge_binding_available
            and facts.knowledge_store_available
            and bool({"kb_read", "kb_search", "kb_grep"}.intersection(loaded)),
            False,
        )
    if key is CapabilityResolverKey.CANVAS_FILES:
        return (
            facts.backend_supports_canvas_presentation and "set_canvas" in loaded,
            False,
        )
    if key is CapabilityResolverKey.CANVAS_BROWSER:
        return (
            facts.backend_supports_shared_browser and "browser_navigate" in loaded,
            False,
        )
    if key is CapabilityResolverKey.SESSION_PROTECTED_CLOUD:
        return (
            facts.protected_cloud_active
            and bool(
                {
                    "copy_file",
                    "create_directory",
                    "delete_directory",
                    "delete_file",
                    "edit_file",
                    "move_file",
                    "rename_file",
                    "write_file",
                }.intersection(loaded)
            ),
            False,
        )

    rule = _TOOL_RULES.get(key)
    if rule is None:
        return False, False
    return (
        bool(rule.execute.intersection(loaded)),
        bool(rule.proposal.intersection(loaded)),
    )


def _unknown_agent_session(
    definition: CapabilityDefinition,
    facts: SessionRuntimeFacts,
) -> tuple[SessionEvaluation, EvaluationError]:
    return (
        _session_result(
            SessionState.UNKNOWN,
            ReasonCode.SESSION_OBSERVATION_UNAVAILABLE,
            facts,
            freshness=Freshness.UNKNOWN,
        ),
        EvaluationError(
            code=EvaluationErrorCode.RESOLVER_ERROR,
            capability_id=definition.id,
            layer=LayerName.SESSION,
            source_component=ProductComponent.AGENT,
            retryable=True,
        ),
    )


def _overlay_live_facts(
    response: ProductCapabilitiesResponse,
    facts: SessionRuntimeFacts,
    *,
    projection_incomplete: bool,
) -> ProductCapabilitiesResponse:
    capabilities: list[ProductCapability] = []
    errors = list(response.evaluation_errors)
    has_guide = "read_product_guide" in facts.loaded_tool_names
    product = merge_product_provenance(
        response.product,
        dict(facts.runtime_component_provenance),
    )

    for capability in response.capabilities:
        definition = CAPABILITY_REGISTRY[capability.id]
        try:
            session = _resolve_live_session(definition, facts)
        except Exception:
            session, error = _unknown_agent_session(definition, facts)
            errors.append(error)

        effective_session = session or capability.session
        has_execute, has_proposal = _action_flags(definition, facts)
        action = derive_agent_action(
            build=capability.build,
            deployment=capability.deployment,
            user=capability.user,
            session=effective_session,
            has_execute_tool=has_execute,
            has_proposal_tool=has_proposal,
            has_guide=has_guide,
        )
        projected_capability = ProductCapability.model_validate(
            {
                **capability.model_dump(mode="python"),
                "session": effective_session,
                "agent_action": action,
            }
        )
        validate_capability_against_definition(projected_capability, definition)
        capabilities.append(projected_capability)

    errors = sorted(
        set(errors),
        key=lambda item: (
            item.capability_id or "",
            item.layer.value if item.layer else "",
            item.code.value,
            item.source_component.value,
            item.retryable,
        ),
    )
    if len(errors) > MAX_EVALUATION_ERRORS:
        errors = errors[:MAX_EVALUATION_ERRORS]
    partial = bool(errors) or response.truncated or projection_incomplete
    return _replace_response(
        response,
        capabilities=capabilities,
        evaluation_errors=errors,
        truncated=(response.truncated or projection_incomplete),
        completeness=(Completeness.PARTIAL if partial else Completeness.COMPLETE),
        product=product,
    )


def _replace_response(
    response: ProductCapabilitiesResponse,
    *,
    capabilities: list[ProductCapability] | None = None,
    evaluation_errors: list[EvaluationError] | None = None,
    truncated: bool | None = None,
    completeness: Completeness | None = None,
    product: ProductProvenance | None = None,
) -> ProductCapabilitiesResponse:
    payload = response.model_dump(mode="python")
    if capabilities is not None:
        payload["capabilities"] = tuple(capabilities)
    if evaluation_errors is not None:
        payload["evaluation_errors"] = tuple(evaluation_errors)
    if truncated is not None:
        payload["truncated"] = truncated
    if completeness is not None:
        payload["completeness"] = completeness
    if product is not None:
        payload["product"] = product
    return ProductCapabilitiesResponse.model_validate(payload)


def _summary(
    response: ProductCapabilitiesResponse,
    *,
    status: CapabilityToolStatus,
) -> str:
    observed_at = response.evaluated_at.isoformat().replace("+00:00", "Z")
    first = (
        f"Product-capability snapshot at {observed_at} is "
        f"{'complete' if status is CapabilityToolStatus.READY else 'partial'}."
    )
    mixed_build = response.product.mixed_build
    build_note = (
        " Component revisions are mixed."
        if mixed_build is True
        else (
            " Known component revisions agree."
            if mixed_build is False
            else " Build uniformity is unknown."
        )
    )
    if not response.capabilities:
        return (
            f"{first}{build_note} "
            "No visible capabilities matched the exact filters; absence does "
            "not prove unsupported, disabled, or denied."
        )

    lines = [f"{first}{build_note}"]
    for capability in response.capabilities:
        qualifiers = [
            qualifier
            for layer in (
                capability.build,
                capability.deployment,
                capability.user,
                capability.session,
            )
            for qualifier in layer.qualifiers
        ]
        qualifier_text = ""
        if qualifiers:
            qualifier_text = "; " + ", ".join(
                f"{item.key}={item.value}" for item in qualifiers
            )
        lines.append(
            f"{capability.id}: "
            f"build={capability.build.state.value}/"
            f"{capability.build.reason_code.value}; "
            f"deployment={capability.deployment.state.value}/"
            f"{capability.deployment.reason_code.value}; "
            f"user={capability.user.state.value}/"
            f"{capability.user.reason_code.value}; "
            f"session={capability.session.state.value}/"
            f"{capability.session.reason_code.value}; "
            f"agent_action={capability.agent_action.value}"
            f"{qualifier_text}"
        )
    return "\n".join(lines)


def _json_output(output: ProductCapabilitiesToolOutput) -> str:
    return json.dumps(
        output.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _bounded_success_output(
    response: ProductCapabilitiesResponse,
    *,
    compatibility: SchemaCompatibility,
    error_code: CapabilityToolErrorCode | None,
) -> ProductCapabilitiesToolOutput:
    status = (
        CapabilityToolStatus.READY
        if response.completeness is Completeness.COMPLETE and error_code is None
        else CapabilityToolStatus.PARTIAL
    )
    output = ProductCapabilitiesToolOutput(
        status=status,
        schema_compatibility=compatibility,
        error_code=error_code,
        summary=_summary(response, status=status),
        response=response,
    )
    if len(_json_output(output).encode("utf-8")) <= MAX_TOOL_OUTPUT_BYTES:
        return output

    capabilities = list(response.capabilities)
    errors = list(response.evaluation_errors)
    size_error = EvaluationError(
        code=EvaluationErrorCode.RESPONSE_SIZE_LIMIT,
        source_component=ProductComponent.AGENT,
        retryable=False,
    )
    if size_error not in errors:
        if len(errors) >= MAX_EVALUATION_ERRORS:
            errors = errors[: MAX_EVALUATION_ERRORS - 1]
        errors.append(size_error)

    while capabilities:
        capabilities.pop()
        bounded = _replace_response(
            response,
            capabilities=capabilities,
            evaluation_errors=errors,
            truncated=True,
            completeness=Completeness.PARTIAL,
        )
        output = ProductCapabilitiesToolOutput(
            status=CapabilityToolStatus.PARTIAL,
            schema_compatibility=compatibility,
            error_code=CapabilityToolErrorCode.OUTPUT_TRUNCATED,
            summary=_summary(bounded, status=CapabilityToolStatus.PARTIAL),
            response=bounded,
        )
        if len(_json_output(output).encode("utf-8")) <= MAX_TOOL_OUTPUT_BYTES:
            return output

    raise _CapabilityToolFailure(CapabilityToolErrorCode.RESPONSE_TOO_LARGE)


def create_product_capability_tools(
    context: ToolContext,
    *,
    fetcher: CapabilityFetcher | None = None,
) -> list[Any]:
    """Create the canary-gated, workspace-independent capability tool."""

    if not product_capabilities_tool_enabled():
        return []
    fetch = fetcher or _fetch_server_payload

    @tool
    async def get_product_capabilities(
        topic: str | None = None,
        capability_ids: list[str] | None = None,
    ) -> str:
        """Check current SRW capability state for this user and session.

        Use exact topic IDs or capability IDs from the product guide/registry.
        The runtime binds the current user and thread; you cannot select another
        identity, project, repository, component, or resolver. The returned
        snapshot explains current state but is not authorization for an action.

        Args:
            topic: Optional exact topic ID, such as ``email`` or ``workspaces``.
            capability_ids: Optional list of at most 12 exact capability IDs.

        Returns:
            Bounded JSON with a deterministic summary and validated per-layer
            capability data, or a closed failure code with availability unknown.
        """

        try:
            capability_request = CapabilityToolRequest(
                topic=topic,
                capability_ids=capability_ids or (),
            )
        except ValidationError:
            return _json_output(
                _safe_failure_output(CapabilityToolErrorCode.INVALID_REQUEST)
            )

        try:
            payload = await fetch(context, capability_request)
            response, compatibility, projection_incomplete = _parse_server_response(
                payload,
                context=context,
                request=capability_request,
            )
            facts = context.session_runtime_facts
            if not isinstance(facts, SessionRuntimeFacts):
                return _json_output(
                    _bounded_success_output(
                        response,
                        compatibility=compatibility,
                        error_code=(CapabilityToolErrorCode.RUNTIME_FACTS_UNAVAILABLE),
                    )
                )
            response = _overlay_live_facts(
                response,
                facts,
                projection_incomplete=projection_incomplete,
            )
            return _json_output(
                _bounded_success_output(
                    response,
                    compatibility=compatibility,
                    error_code=(
                        CapabilityToolErrorCode.PROJECTION_INCOMPLETE
                        if projection_incomplete
                        else None
                    ),
                )
            )
        except _CapabilityToolFailure as exc:
            return _json_output(
                _safe_failure_output(
                    exc.code,
                    compatibility=exc.compatibility,
                )
            )
        except httpx.TimeoutException:
            return _json_output(
                _safe_failure_output(CapabilityToolErrorCode.ENDPOINT_TIMEOUT)
            )
        except httpx.HTTPError:
            return _json_output(
                _safe_failure_output(CapabilityToolErrorCode.ENDPOINT_UNAVAILABLE)
            )
        except Exception as exc:
            logger.warning(
                "Product capability tool failed closed (%s)",
                type(exc).__name__,
            )
            return _json_output(
                _safe_failure_output(CapabilityToolErrorCode.INVALID_RESPONSE)
            )

    return [get_product_capabilities]


__all__ = [
    "CAPABILITY_REQUEST_TIMEOUT_SECONDS",
    "CapabilityToolErrorCode",
    "CapabilityToolRequest",
    "CapabilityToolStatus",
    "MAX_SERVER_RESPONSE_BYTES",
    "MAX_TOOL_CAPABILITY_IDS",
    "MAX_TOOL_OUTPUT_BYTES",
    "PRODUCT_CAPABILITIES_TOOL_ENABLED_ENV",
    "PRODUCT_CAPABILITIES_TOOL_NAME",
    "ProductCapabilitiesToolOutput",
    "create_product_capability_tools",
    "get_product_capabilities_metadata",
    "product_capabilities_tool_enabled",
]
