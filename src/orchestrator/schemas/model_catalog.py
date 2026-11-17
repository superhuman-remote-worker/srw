"""Curated model catalogue request contracts and capability vocabulary."""

from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator

from shared.subscription_routing import validate_routing_params


LLM_MODEL_CAPABILITIES = (
    "chat",
    "vision",
    "embedding",
    "auxiliary",
    "whisper",
    "tts",
    "search",
    "fetch",
    "rerank",
    "live_voice",
    "image_generation",
)


# Locked enum for the admin-curated catalog. Adding a new capability requires
# touching every consumer (resolver, dispatcher, default-model fallback),
# so the schema-level CHECK constraint and this Literal are kept in sync.
# ``rerank`` is the memory reranker's slot (a Cohere-shaped ``/rerank`` route;
# migration 0240). It is wired like ``embedding`` — required by the readiness
# gate, one default pin, flat ``RERANK_*`` env at dispatch — until embedding
# and rerank become optional together.
# ``live_voice`` is a full-duplex voice model (GPT-Live; migration 0536). A
# phone-line connector names it; it is never bound as a chat model, never
# required by readiness and never injected at dispatch. It is the row's only
# capability (see :func:`live_voice_capability_problem`).
# ``image_generation`` is a text-to-image / image-edit model (gpt-image,
# Gemini image, the subscription proxy's image routes; migration 0571). The
# agent's ``generate_image`` tool calls it through the ``image`` dispatch
# section. Like ``live_voice`` it is the row's only capability, is never
# bound as a chat or vision model, never required by readiness, and never
# picked by the first-enabled default fallback: it needs an explicit pin.
VALID_CATALOG_CAPABILITIES = (
    "chat",
    "auxiliary",
    "embedding",
    "vision",
    "whisper",
    "tts",
    "search",
    "fetch",
    "rerank",
    "live_voice",
    "image_generation",
)

#: Capabilities a catalog row must carry alone (a contradiction otherwise).
EXCLUSIVE_CATALOG_CAPABILITIES = ("live_voice", "image_generation")

#: Capabilities whose default never falls back to the first enabled row: the
#: slot stays empty until an admin (or the user) pins a model, because an
#: enabled row of this kind spends money per call (image_generation decision
#: D1, 2026-11-17).
PIN_ONLY_CAPABILITIES = ("image_generation",)


VALID_CATALOG_PROVIDER_KINDS = ("system", "endpoint")


CatalogCapabilityLiteral = Literal[
    "chat",
    "auxiliary",
    "embedding",
    "vision",
    "whisper",
    "tts",
    "search",
    "fetch",
    "rerank",
    "live_voice",
    "image_generation",
]


def exclusive_capability_problem(capabilities: list[str] | None) -> str | None:
    """Why a capability set that claims an exclusive capability contradicts itself.

    A live voice model speaks to a caller over a transport, and an image
    model returns pictures; neither can also serve as a chat, embedding or
    any other slot, so each is the row's only capability.
    """
    caps = list(capabilities or [])
    for cap in EXCLUSIVE_CATALOG_CAPABILITIES:
        if cap in caps and len(set(caps)) > 1:
            return f"{cap} must be the only capability of a catalog row"
    return None


def live_voice_capability_problem(capabilities: list[str] | None) -> str | None:
    """Kept name: the exclusive-capability check (``live_voice`` and
    ``image_generation``)."""
    return exclusive_capability_problem(capabilities)


class CatalogModelCreate(BaseModel):
    """Request body for inserting a catalog row (Admin → Models).

    ``capabilities`` is the source of truth — one row can serve multiple
    roles (e.g. ``['chat', 'auxiliary']`` for a chat-capable LLM,
    ``['chat', 'auxiliary', 'vision']`` for a multimodal one). The legacy
    singular ``capability`` form is no longer accepted; clients post the
    array directly.
    """

    provider_kind: Literal["system", "endpoint"]
    provider_ref: str = Field(
        ...,
        min_length=1,
        description=(
            "system_api_keys.provider slug for provider_kind='system' "
            "(e.g. 'anthropic'); llm_endpoints.id (UUID as text) for "
            "provider_kind='endpoint'."
        ),
    )
    model_id: str = Field(..., min_length=1, max_length=500)
    served_model_id: str | None = Field(
        None,
        max_length=500,
        description=(
            "The name sent to the provider when it differs from model_id "
            "(e.g. model_id 'claude-opus-5-5-api' serving 'claude-opus-5-5'). "
            "model_id must then be this name plus a suffix of the same "
            "family. Null or empty sends model_id."
        ),
    )
    display_label: str = Field(..., min_length=1, max_length=200)
    capabilities: list[CatalogCapabilityLiteral] = Field(
        ...,
        min_length=1,
        description=(
            "The set of capabilities this model row claims. One row can "
            "serve multiple roles (e.g. ['chat', 'auxiliary'] for a "
            "chat-capable LLM, ['chat', 'auxiliary', 'vision'] for a "
            "multimodal one). Must be non-empty."
        ),
    )
    family: str = Field(
        ...,
        min_length=1,
        description="model_config_matrix.yaml key (e.g. 'claude-opus', 'gemini').",
    )
    context_window: int | None = Field(
        None,
        description=(
            "Optional override; falls back to model_config_matrix family "
            "default. Pass null (the default) to use the matrix; pass an "
            "explicit int to override (zero is allowed and round-trips as "
            "zero)."
        ),
    )
    reasoning_level: str | None = None
    params_json: dict[str, Any] | None = Field(
        None,
        description=(
            "Optional inference param overrides (e.g. {'temperature': 0.0}). "
            "Null means 'use family defaults'; explicit zero/false values "
            "round-trip as themselves (create_model accessor regression guard)."
        ),
    )
    enabled: bool = True
    notes: str | None = None

    @field_validator("capabilities")
    @classmethod
    def _live_voice_alone(cls, value: list[str]) -> list[str]:
        problem = live_voice_capability_problem(value)
        if problem:
            raise ValueError(problem)
        return value

    @model_validator(mode="after")
    def _routing_block_is_valid(self) -> "CatalogModelCreate":
        validate_routing_params(self.params_json, provider_kind=self.provider_kind)
        return self


class CatalogModelUpdate(BaseModel):
    """Partial update — only fields explicitly set in the request body are
    applied. Pass ``null`` to clear an optional column to NULL.
    """

    provider_kind: Literal["system", "endpoint"] | None = None
    provider_ref: str | None = Field(None, min_length=1)
    model_id: str | None = Field(None, min_length=1, max_length=500)
    served_model_id: str | None = Field(None, max_length=500)
    display_label: str | None = Field(None, min_length=1, max_length=200)
    capabilities: list[CatalogCapabilityLiteral] | None = Field(None, min_length=1)
    family: str | None = Field(None, min_length=1)
    context_window: int | None = None
    reasoning_level: str | None = None
    params_json: dict[str, Any] | None = None
    enabled: bool | None = None
    notes: str | None = None

    @field_validator("capabilities")
    @classmethod
    def _live_voice_alone(cls, value: list[str] | None) -> list[str] | None:
        problem = live_voice_capability_problem(value)
        if problem:
            raise ValueError(problem)
        return value

    @field_validator("params_json")
    @classmethod
    def _routing_block_is_valid(
        cls, value: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        # The row's provider_kind is not in a partial body; the service
        # re-checks the endpoint-only rule against the stored row.
        validate_routing_params(value)
        return value
