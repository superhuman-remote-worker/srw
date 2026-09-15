"""Model registry — single source of truth for model routing metadata.

Resolves a model ID to a ModelMeta (provider, family, base_url, api_key_ref,
etc.) via three DB-backed sources:

1. The per-user ``user_llm_endpoint_models`` table, queried through a
   callable installed by ``register_custom_lookup`` at orchestrator startup
   (``origin='custom'``).
2. System-scoped rows in the same table (``user_id IS NULL``), queried
   through a callable installed by ``register_system_lookup``
   (``origin='system'``).
3. The admin-curated ``models`` catalog table, queried through a callable
   installed by ``register_catalog_lookup`` (``origin='catalog'``).

Unknown IDs raise ``UnknownModelError``. Dispatch code consumes the
ModelMeta to decide which LLM factory to invoke and whether to inject a
``base_url`` / ``api_key`` into the job's ``config_override``.

The legacy YAML fallback (``config/models.yaml`` + ``LLM_BASE_URL``
env-var inheritance for self-hosted "Local" group models) was removed in
chunk 6 of the ``models_yaml_removal`` work. Self-hosted models are now
catalog rows pointing at an explicit ``llm_endpoints`` transport — the
previous "fall through to api.openai.com with `not-needed`" silent
failure mode is structurally impossible.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from shared.subscription_routing import (
    LEGACY_CODEX_PROXY_ENDPOINT_LABEL,
    RoutingMetadata,
    applies_codex_context_cap,
    factory_provider_for_protocol,
    is_subscription_endpoint,
    protocol_for_factory_provider,
    routing_from_params,
)

logger = logging.getLogger(__name__)


class UnknownModelError(LookupError):
    """Raised when a model ID is not found in any registry source."""

    def __init__(self, model_id: str) -> None:
        super().__init__(
            f"Unknown model '{model_id}'. Configure it via Admin → Models "
            f"(a catalog row anchored to a system API key or a system endpoint "
            f"registered under Admin → Providers)."
        )
        self.model_id = model_id


@dataclass(frozen=True)
class ModelMeta:
    """Routing + display metadata for a single model."""

    model_id: str
    provider: str
    family: str
    display_name: str
    base_url: Optional[str] = None
    api_key_ref: Optional[str] = None
    context_window: Optional[int] = None
    # Per-model output cap (Admin → Models, stored in models.params_json). Seeded
    # into dispatch the same way context_window is, then it overrides the family
    # settings.max_output_tokens before _resolve_max_output_tokens clamps to the
    # context backstop. See knowledge-base/knowledge/features/reasoning_aware_max_output_tokens.md §5.2.
    max_output_tokens: Optional[int] = None
    reasoning_level: Optional[str] = None
    origin: str = "catalog"
    endpoint_id: Optional[str] = None
    capability: str = "chat"
    # Explicit routing metadata (knowledge-base/knowledge/features/subscription_proxy.md
    # §6). ``transport_kind`` marks the endpoint as the shared subscription
    # proxy independently of its label/hostname; ``client_protocol`` is the API
    # schema SRW sends; ``subscription_sources`` is the upstream credential
    # channel provenance (many-to-many — a pooled model can be served by more
    # than one connected account).
    transport_kind: Optional[str] = None
    client_protocol: Optional[str] = None
    subscription_sources: tuple[str, ...] = ()


_FACTORY_PROVIDERS = {
    "openai",
    "anthropic",
    "google",
    "groq",
    "openrouter",
    "mistral",
    "codex",
}


def _factory_provider(yaml_provider: Optional[str]) -> str:
    """Map a provider label to the LLM factory that serves it.

    Catalog rows use the same provider slugs as the legacy YAML (``local``
    for self-hosted OpenAI-compatible endpoints — those route through the
    openai factory because the wire protocol matches; the distinction only
    matters for UI filtering and access control, not for dispatch).
    """
    if yaml_provider is None or yaml_provider == "local":
        return "openai"
    if yaml_provider in _FACTORY_PROVIDERS:
        return yaml_provider
    return "openai"


# Pre-rename label of the seeded subscription-proxy row. Kept as a module
# constant because callers (and tests) still import it; the *authority* for
# "is this the subscription proxy" is now ``llm_endpoints.transport_kind``
# (see shared.subscription_routing). The proxy's Responses path surfaces model
# reasoning via ``reasoning.summary``, which lives in the codex factory
# (``_create_codex_llm``); the generic ``openai`` (Chat Completions) factory
# forces ``use_responses_api=False`` and never requests a reasoning summary, so
# a mis-resolved row silently loses gpt-5.x reasoning.
CODEX_PROXY_ENDPOINT_LABEL = LEGACY_CODEX_PROXY_ENDPOINT_LABEL


def _endpoint_factory_provider(
    base_url: Optional[str],
    label: Optional[str] = None,
    *,
    transport_kind: Optional[str] = None,
    routing: Optional[RoutingMetadata] = None,
) -> str:
    """Pick the agent-side LLM factory for an endpoint-backed row.

    Resolution order (knowledge-base/knowledge/features/subscription_proxy.md §6):

    1. **Explicit per-model client protocol.** ``openai-responses`` → the
       ``codex`` factory, ``openai-chat`` → the generic ``openai`` factory.
       This is the only rule that can express "Claude Code over Chat
       Completions on the same endpoint as Codex over Responses".
    2. **Subscription transport with no explicit protocol.** The pre-feature
       Codex install: every model on that endpoint went through the Responses
       factory, so keep doing that. The migration backfills rule 1 onto those
       rows, and this branch is the safety net for a row created before the
       backfill (or after a schema rollback).
    3. Anything else is an ordinary OpenAI-compatible endpoint.
    """
    protocol_factory = factory_provider_for_protocol(
        routing.client_protocol if routing else None
    )
    if protocol_factory:
        return protocol_factory
    if is_subscription_endpoint(
        transport_kind=transport_kind, label=label, base_url=base_url
    ):
        return "codex"
    return "openai"


# OpenAI's Codex product surface caps context far below the models' true API
# windows — a deliberate throughput/cost limit, not a capability. gpt-5.x report
# ~1M on the raw API, but the Codex/ChatGPT-OAuth backend the system codex proxy
# speaks rejects larger inputs with ``context_too_large``. Measured live against
# srw-codex-proxy 2026-07-10: 356,590 input -> 200, 380,364 input -> 400. We
# declare this as the working window so context compaction (threshold = 80% of
# it) keeps input under the wall instead of dead-ending every turn on an
# empty/400. Applies to any model resolved onto the ``codex`` factory, regardless
# of family or catalog window. Override via ``CODEX_CONTEXT_WINDOW_CAP`` (set 0 to
# disable the day OpenAI ships 1M-for-Codex — see openai/codex#19464).
CODEX_CONTEXT_WINDOW_CAP_DEFAULT = 400_000


def _codex_context_cap() -> int:
    """Effective context ceiling for codex-proxy-routed models (env-overridable).

    Read from ``CODEX_CONTEXT_WINDOW_CAP``; ``<= 0`` disables the clamp (fall back
    to the model's own window). A malformed value falls back to the default rather
    than 0, so a typo can't silently un-cap and re-wedge sessions.
    """
    try:
        return int(
            os.getenv("CODEX_CONTEXT_WINDOW_CAP", str(CODEX_CONTEXT_WINDOW_CAP_DEFAULT))
        )
    except (TypeError, ValueError):
        return CODEX_CONTEXT_WINDOW_CAP_DEFAULT


def _family_context_window(model_id: Optional[str]) -> Optional[int]:
    """The family matrix's declared true window for ``model_id`` (None if absent).

    Deferred import: ``loader`` imports ``family_of`` from this module, so the
    dependency can only run in this direction at call time. Any failure degrades
    to None — the caller then keeps the historical NULL->cap behaviour.
    """
    if not model_id:
        return None
    try:
        from shared.runtime.core.loader import resolve_model_settings

        window = resolve_model_settings(model_id).get("model_max_context_tokens")
        return int(window) if window else None
    except Exception as e:  # pragma: no cover - defensive
        logger.debug(f"Family window lookup failed for {model_id}: {e}")
        return None


def _cap_context_window(
    provider: str,
    context_window: Optional[int],
    model_id: Optional[str] = None,
    subscription_sources: tuple[str, ...] = (),
) -> Optional[int]:
    """Clamp a Codex-sourced model's working window to the Codex surface cap.

    The clamp belongs to *OpenAI's Codex/ChatGPT-OAuth backend*, not to the
    Responses API and not to the proxy as a whole. It therefore applies to a
    route whose ``subscription_sources`` include ``codex``, and to a route with
    unknown sources that still resolved onto the ``codex`` factory — the
    pre-feature Codex install, whose behaviour must not change on upgrade.
    A Grok Build / Kimi / Claude Code / Antigravity route with known sources
    keeps its own window even though it may share the same proxy endpoint (and,
    for Grok Build, the same Responses protocol). Everything else passes
    through untouched.

    Where it does apply the cap is a **ceiling, never a floor**: the effective
    window is the admin ``context_window`` when set, else the family matrix's
    declared true window, and whichever applies is then ``min``'d with the cap.
    Only when neither is known does the cap itself stand in. Keying on the
    resolved *route*, not the family, means gpt-5.x over the real API keeps its
    full window while the same model over the subscription proxy is capped.

    The family fallback matters for models whose true window is *below* the cap.
    ``gpt-5.3-codex-spark`` is a distilled 128K model: with a NULL catalog row the
    old ``NULL -> cap`` rule handed back 400K, and because that value is injected
    at dispatch into ``llm.model_max_context_tokens`` it is truthy, so
    ``loader._apply_settings_matrix`` never reached the matrix's correct 128000.
    The 80% compaction threshold landed at 320K, compaction never fired, and the
    job hard-400'd on ``context_too_large`` (job 9a99f433, 2026-07-23).
    """
    if not applies_codex_context_cap(provider, subscription_sources):
        return context_window
    cap = _codex_context_cap()
    if cap <= 0:
        return context_window
    effective = context_window or _family_context_window(model_id)
    return min(effective, cap) if effective else cap


# Dependency injection: the orchestrator registers DB-backed lookups at
# startup so src/core/ stays import-free of orchestrator/database/. In
# contexts without a DB (agent process, tests), the hooks stay None and
# resolve_model() skips straight to the built-in catalog.
CustomLookup = Callable[..., Awaitable[Optional[dict[str, Any]]]]
SystemLookup = Callable[..., Awaitable[Optional[dict[str, Any]]]]
CatalogLookup = Callable[..., Awaitable[Optional[dict[str, Any]]]]
_custom_lookup: Optional[CustomLookup] = None
_system_lookup: Optional[SystemLookup] = None
_catalog_lookup: Optional[CatalogLookup] = None


def register_custom_lookup(fn: Optional[CustomLookup]) -> None:
    """Install (or clear) the per-user custom-endpoint lookup callable.

    The callable takes (user_id, model_id, capability) and returns either
    None or a row dict with keys: endpoint_id, base_url, api_key, model_id,
    display_name, family, context_window, reasoning_level, capability.
    Typically wired to ``postgres_db.resolve_user_llm_model`` at orchestrator
    startup, whose ``capability`` parameter defaults to ``'chat'``.
    """
    global _custom_lookup
    _custom_lookup = fn


def register_system_lookup(fn: Optional[SystemLookup]) -> None:
    """Install (or clear) the system-scope endpoint lookup callable.

    The callable takes (model_id, capability) and returns either None or a
    row dict with the same shape as the custom lookup (minus user_id).
    Wired to ``postgres_db.resolve_system_llm_model`` at orchestrator
    startup, whose ``capability`` parameter defaults to ``'chat'``.
    """
    global _system_lookup
    _system_lookup = fn


def register_catalog_lookup(fn: Optional[CatalogLookup]) -> None:
    """Install (or clear) the DB-backed catalog lookup callable.

    The callable takes (model_id, capability='chat') and returns either None
    or a flattened row dict from the ``models`` table joined to its transport
    (system_api_keys or llm_endpoints). Wired to
    ``postgres_db.resolve_catalog_model`` at orchestrator startup.
    """
    global _catalog_lookup
    _catalog_lookup = fn


def family_of(model_id: str, default: str = "default") -> str:
    """Return the family string for a model ID.

    The DB-backed catalog row carries ``family`` explicitly; this helper
    is the sync prefix-pattern fallback used by call sites that don't have
    a row in hand (settings-matrix family lookup before resolution, log
    formatting, etc.).

    The heuristic strips common provider prefixes (``openrouter/``,
    ``groq/``, ``codex/``, ``openai/``) and pattern-matches against the
    known family names. Deliberately minimal and explicit-loss: returns
    ``default`` on any miss rather than inventing new families.
    """
    name = model_id.lower()
    for prefix in ("openrouter/", "groq/", "codex/"):
        if name.startswith(prefix):
            name = name[len(prefix) :]
            if "/" in name:
                name = name.split("/", 1)[1]
            break
    if name.startswith("openai/"):
        name = name[len("openai/") :]

    # Opus 5 before the generic Opus rule: it is the only Opus that takes the
    # full effort ladder (xhigh/max), so it carries its own matrix family.
    # family_matcher.detect_family orders its rules the same way.
    if name.startswith("claude-opus-5"):
        return "claude-opus-5"
    if name.startswith("claude-opus"):
        return "claude-opus"
    if name.startswith("claude-sonnet"):
        return "claude-sonnet"
    if name.startswith("claude-haiku"):
        return "claude-haiku"
    # Fable 5 and 5.1 share one family — same context, output cap, vision and
    # effort ladder; nothing this matrix carries differs between them.
    if name.startswith("claude-fable"):
        return "claude-fable"
    if "codex-spark" in name:
        return "codex-spark"
    if "codex" in name and name.startswith(("gpt-5", "gpt-6")):
        return "codex"
    # GPT-6 (Astra) — a single flagship row today, no tier suffixes. Must sit
    # below the codex checks (which now also cover a future gpt-6 codex
    # variant) so the two resolvers agree; family_matcher.detect_family orders
    # its rules the same way.
    if name.startswith("gpt-6"):
        return "gpt-6"
    if name.startswith("gpt-5.6"):
        return "gpt-5.6"
    if name.startswith("gpt-5"):
        return "gpt-5"
    if name.startswith("gpt-4o"):
        return "gpt-4o"
    if name.startswith(("o1", "o3", "o4")):
        return "o-series"
    if "deepseek" in name:
        return "deepseek"
    # Flash has vision; the flagship is text-only. Both have a narrower
    # reasoning ladder than older GLM models, so match before generic glm.
    if "glm-5.3-flash" in name:
        return "glm-5.3-flash"
    if "glm-5.3" in name:
        return "glm-5.3"
    if "glm" in name:
        return "glm"
    # Version-specific: older Muse releases must not inherit 1.3 capabilities.
    if re.search(r"(?:^|/)muse-spark-1\.3(?:$|[-:])", name):
        return "muse-spark-1.3"
    if name.startswith(
        (
            "mistral",
            "codestral",
            "magistral",
            "ministral",
            "devstral",
            "pixtral",
            "voxtral",
        )
    ):
        # Mistral 3 family + specialists. Native api.mistral.ai serves bare ids
        # (mistral-large-latest, codestral-latest); the openrouter/ prefix is
        # stripped above. All share the `mistral` matrix family.
        return "mistral"
    if "qwen" in name or "qwq" in name:
        return "qwen"
    if "llama" in name:
        return "llama"
    if name.startswith("gemini"):
        return "gemini"
    if name.startswith("gpt-oss"):
        return "gpt-oss"
    if "gemma" in name:
        return "gemma"
    if "minimax" in name:
        # M3 (MSA architecture, 1M context, native multimodal) ships its own
        # prompt/settings family and must win over the generic minimax match.
        if "m3" in name:
            return "minimax-m3"
        return "minimax"

    return default


def _params_max_output_tokens(row: dict[str, Any]) -> Optional[int]:
    """Per-model output cap from a catalog row's ``params_json`` (Admin → Models).

    Seeded into dispatch alongside ``context_window`` so it overrides the family
    ``settings.max_output_tokens`` (then clamped to the context backstop by
    ``_resolve_max_output_tokens``). Returns None for a missing, non-int, or
    non-positive value (→ fall back to the family value). Catalog rows arrive with
    ``params_json`` already parsed (``postgres._row_to_model``); an endpoint-hook
    row that left it a raw JSON string is treated as 'no override', not parsed here.
    """
    pj = row.get("params_json")
    if not isinstance(pj, dict):
        return None
    try:
        ival = int(pj.get("max_output_tokens"))
    except (TypeError, ValueError):
        return None
    return ival if ival > 0 else None


def _endpoint_row_to_meta(row: dict[str, Any], *, origin: str) -> ModelMeta:
    """Build a ModelMeta from a user/system endpoint lookup row.

    Endpoint-backed models route through the openai factory (the wire
    protocol is OpenAI-compatible) — except rows carrying an explicit
    ``openai-responses`` client protocol and rows on the shared subscription
    proxy with no explicit protocol, which need the codex factory's
    Responses-API + reasoning-summary path (see ``_endpoint_factory_provider``).
    api_key_ref is None because the key travels inline on the endpoint row —
    the dispatcher fetches it via get_user_llm_endpoint(endpoint_id), not
    through resolve_api_keys_for_job.
    """
    routing = routing_from_params(row.get("params_json"))
    transport_kind = row.get("transport_kind") or row.get("endpoint_transport_kind")
    provider = _endpoint_factory_provider(
        row.get("base_url"),
        row.get("label"),
        transport_kind=transport_kind,
        routing=routing,
    )
    return ModelMeta(
        model_id=row["model_id"],
        provider=provider,
        family=row.get("family") or "default",
        display_name=row.get("display_name") or row["model_id"],
        base_url=row["base_url"],
        api_key_ref=None,
        context_window=_cap_context_window(
            provider,
            row.get("context_window"),
            row["model_id"],
            routing.subscription_sources,
        ),
        max_output_tokens=_params_max_output_tokens(row),
        reasoning_level=row.get("reasoning_level"),
        origin=origin,
        endpoint_id=str(row["endpoint_id"]),
        capability=row.get("capability") or "chat",
        transport_kind=transport_kind,
        client_protocol=routing.client_protocol
        or protocol_for_factory_provider(provider),
        subscription_sources=routing.subscription_sources,
    )


# Kept for backwards-compat with any external imports. Prefer
# ``_endpoint_row_to_meta`` in new code.
def _custom_row_to_meta(row: dict[str, Any]) -> ModelMeta:
    return _endpoint_row_to_meta(row, origin="custom")


def _catalog_row_to_meta(row: dict[str, Any]) -> ModelMeta:
    """Build a ModelMeta from a ``models`` catalog row joined to its transport.

    Two shapes:
    - ``provider_kind='endpoint'`` — inherits ``base_url`` + ``api_key``
      from the joined ``llm_endpoints`` row, and its routing metadata from
      the row's own ``params_json`` plus the endpoint's ``transport_kind``.
    - ``provider_kind='system'`` — sets ``api_key_ref`` to the provider
      slug so the dispatcher resolves the key via ``system_api_keys``;
      ``base_url`` stays None so the factory uses its hardcoded default.
      Never a subscription route, so it carries no routing metadata.
    """
    provider_kind = row["provider_kind"]
    provider_ref = row["provider_ref"]
    capability = row.get("capability") or "chat"
    if provider_kind == "endpoint":
        routing = routing_from_params(row.get("params_json"))
        transport_kind = row.get("endpoint_transport_kind")
        provider = _endpoint_factory_provider(
            row.get("endpoint_base_url"),
            row.get("endpoint_label"),
            transport_kind=transport_kind,
            routing=routing,
        )
        return ModelMeta(
            model_id=row["model_id"],
            provider=provider,
            family=row.get("family") or "default",
            display_name=row.get("display_label") or row["model_id"],
            base_url=row.get("endpoint_base_url"),
            api_key_ref=None,
            context_window=_cap_context_window(
                provider,
                row.get("context_window"),
                row["model_id"],
                routing.subscription_sources,
            ),
            max_output_tokens=_params_max_output_tokens(row),
            reasoning_level=row.get("reasoning_level"),
            origin="catalog",
            endpoint_id=str(row["endpoint_id"]) if row.get("endpoint_id") else None,
            capability=capability,
            transport_kind=transport_kind,
            client_protocol=routing.client_protocol
            or protocol_for_factory_provider(provider),
            subscription_sources=routing.subscription_sources,
        )
    provider = _factory_provider(provider_ref)
    return ModelMeta(
        model_id=row["model_id"],
        provider=provider,
        family=row.get("family") or "default",
        display_name=row.get("display_label") or row["model_id"],
        base_url=None,
        api_key_ref=provider_ref,
        context_window=_cap_context_window(
            provider, row.get("context_window"), row["model_id"]
        ),
        max_output_tokens=_params_max_output_tokens(row),
        reasoning_level=row.get("reasoning_level"),
        origin="catalog",
        capability=capability,
    )


async def resolve_model(
    model_id: str,
    user_id: Optional[str] = None,
    capability: str = "chat",
) -> ModelMeta:
    """Resolve a model ID to its routing metadata.

    Resolution order:
        1. Per-user custom endpoints (when user_id is set and the custom
           lookup hook has been registered).
        2. System-scoped endpoints (when the system lookup hook has been
           registered). Shared across all users; seeded by helm or
           managed via Admin → Providers.
        3. DB-backed catalog (``models`` table) — admin-curated offerings
           anchored to a transport (system_api_keys or system endpoint).
           When matched, the row's transport is joined inline so the
           returned ModelMeta carries either ``base_url`` (endpoint) or
           ``api_key_ref`` (system provider).
        4. Miss → UnknownModelError.

    Args:
        model_id: Model identifier (e.g., "claude-opus-4-6",
            "RedHatAI/gemma-4-31B-it-FP8-Dynamic").
        user_id: Optional user UUID. Used for custom-endpoint lookup when
            a hook is registered; ignored otherwise.
        capability: Which capability slot to resolve ('chat', 'vision',
            'embedding', 'auxiliary', 'whisper'). Defaults to 'chat' so
            existing callers resolving chat-completion models keep working
            unchanged; non-chat resolvers pass explicitly.

    Returns:
        ModelMeta with provider/family/routing fields populated.

    Raises:
        UnknownModelError: Model ID not found in any source.
    """
    if user_id and _custom_lookup is not None:
        row = await _custom_lookup(user_id, model_id, capability)
        if row is not None:
            return _endpoint_row_to_meta(row, origin="custom")

    if _system_lookup is not None:
        row = await _system_lookup(model_id, capability)
        if row is not None:
            return _endpoint_row_to_meta(row, origin="system")

    if _catalog_lookup is not None:
        row = await _catalog_lookup(model_id, capability=capability)
        if row is not None:
            return _catalog_row_to_meta(row)

    raise UnknownModelError(model_id)
