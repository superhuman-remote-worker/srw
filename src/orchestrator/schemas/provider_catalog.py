"""Provider key, endpoint and default model request contracts."""

from pydantic import BaseModel, Field


VALID_API_KEY_PROVIDERS = {
    "openai",
    "anthropic",
    "google",
    "groq",
    "openrouter",
    "mistral",
    "codex",
    "vision",
}


class ApiKeySet(BaseModel):
    """Request body for setting an API key for a provider."""

    api_key: str = Field(..., min_length=1, description="The API key value")
    label: str | None = Field(
        None, description="Optional label (e.g. 'team key', 'personal')"
    )


class LlmEndpointCreate(BaseModel):
    """Request body for registering a new LLM endpoint.

    The endpoint must be OpenAI-compatible (vLLM, Ollama, private gateway).
    ``base_url`` should be the full OpenAI path prefix, e.g.
    ``https://my-vllm.example/v1``. ``api_key`` is optional — some local
    servers don't require auth.
    """

    label: str = Field(..., min_length=1, max_length=200)
    base_url: str = Field(..., min_length=1)
    api_key: str | None = None
    allow_insecure: bool = Field(
        False,
        description=(
            "Opt-in for http:// URLs. Default rejects non-HTTPS to guard "
            "against copy-paste accidents."
        ),
    )


class LlmEndpointUpdate(BaseModel):
    """Partial update — only non-None fields are applied.

    ``clear_api_key=True`` nulls the stored key (for endpoints that
    transition from authenticated to anonymous). Ignored when ``api_key``
    is also set.
    """

    label: str | None = None
    base_url: str | None = None
    api_key: str | None = None
    clear_api_key: bool = False
    allow_insecure: bool = False


class AdminDefaultModelSet(BaseModel):
    """Request body for setting a default LLM model on the system.

    ``model`` is the model ID to resolve via the registry (e.g.
    ``RedHatAI/gemma-4-31B-it-FP8-Dynamic``, ``gpt-4o``). Pass an empty
    string to clear the default.
    """

    model: str = Field(..., description="Model ID; empty string clears the default")


# Slots admins can pin cluster-wide via Admin → Providers → Defaults. The
# system_settings key pattern is ``llm.default_<kind>_model``. ``tts`` is
# present even without a current consumer in src/services/ — landing the
# plumbing keeps the registry path uniform across audio capabilities.
#
# The ``chat`` slot is the cluster-wide chat default — used by the
# orchestrator dispatcher when a job/session doesn't carry its own model
# override (see resolve_default_for_capability("chat") at the dispatch
# call sites). Surfacing it in the cockpit's Defaults panel lets the
# readiness gate's ``Pin a default for: chat`` requirement actually have
# a UI to fulfill (it was previously phantom — the gate asked but the
# panel didn't render the dropdown).
VALID_DEFAULT_MODEL_KINDS = {
    "chat",
    "browser",
    "citation",
    "embedding",
    "vision",
    "auxiliary",
    "whisper",
    "tts",
    "search",
    "fetch",
    "search_fallback",
    "rerank",
}


# System-scoped API keys only cover shared providers. Codex auth is
# user-bound through the proxy and isn't appropriate for a system key.
VALID_SYSTEM_API_KEY_PROVIDERS = {
    "openai",
    "anthropic",
    "google",
    "groq",
    "openrouter",
    "vision",
}


class SubscriptionModelImport(BaseModel):
    """Request body for bulk-registering discovered subscription models."""

    model_ids: list[str] | None = Field(
        None,
        description=(
            "Wire IDs to register, exactly as advertised. Omit to register "
            "every supported model ('Add all supported models')."
        ),
    )
    include_needs_review: bool = Field(
        False,
        description=(
            "Also register candidates whose protocol/source could not be "
            "resolved. They are stored flagged for review, on the neutral "
            "Chat Completions protocol."
        ),
    )
    context_window_cap: int | None = Field(
        None,
        gt=0,
        description=(
            "Upper bound for the imported rows' context window. Each row gets "
            "the smaller of this and the model's advertised maximum. Omit to "
            "store the advertised maximum unchanged."
        ),
    )
