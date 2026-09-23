"""Per-request credential and model-slot resolution for dispatch.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane C, census groups
``R_CREDENTIALS`` and ``R_PROVIDER``). This module owns *policy over* the
existing credential authorities, never a second copy of them:

* ``services/provider_credentials.py`` remains the store for user/project
  provider keys; the resolved key map still arrives from
  ``store.resolve_api_keys_for_job``.
* ``services/capability_credentials.py`` remains the resolver for
  capability-scoped rows (search / fetch / tts / transcribe);
  :func:`inject_search_credentials` selects and shapes what it returns and
  resolves it through a *function-local* import so a test patching
  ``orchestrator.services.capability_credentials.resolve_capability_credentials``
  is still reached.
* ``shared.runtime.core.model_registry.resolve_model`` remains the catalog
  resolver; it arrives as an injected callable rather than a module-level
  import so the application's late binding (and the suites that monkeypatch
  ``orchestrator.main._resolve_model``) still steer every call site here.

Three properties this module exists to keep, stated so they stay testable:

1. **A credential reaches only the section it was resolved for.** Nothing here
   writes a key into a response body, and every log statement names a model, a
   provider, a capability, an endpoint id or a failure kind — never a key, a
   token or a resolved transport secret.
2. **Resolution precedence.** A caller-pinned value wins (every provider-key
   write is ``setdefault``); an endpoint row is authoritative for its own
   transport and deliberately replaces a stale persisted one; the resolved key
   map is the last resort. Selection order for a *model* is: the section's own
   pin, then the user setting, then the system capability default.
3. **The fallback is a fallback.** :func:`dispatch_llm_provider_fallback` and
   :func:`provider_of_model` are consulted only where registry resolution
   produced nothing.

Collaborators that the application rebinds during ``lifespan`` (the store, the
logger) arrive through :class:`DispatchCredentialDependencies`, built per
invocation by a main factory — never captured at import (R1.B05 §P1).
"""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from shared.runtime.core.loader import INHERIT_MODEL, canonical_config_name
from shared.runtime.core.model_registry import UnknownModelError
from shared.runtime.core.transport_resolution import env_endpoint_names
from shared.subscription_routing import subscription_request_headers


@dataclass(frozen=True)
class DispatchCredentialDependencies:
    """Collaborators for one credential-injection call, resolved per invocation.

    ``store`` is the application ``PostgresDB`` (``get_user_llm_endpoint``,
    ``resolve_api_keys_for_job``, ``resolve_default_for_capability``).
    ``resolve_model`` is ``shared.runtime.core.model_registry.resolve_model`` as
    the application currently binds it — injected rather than imported so a
    late rebind or a monkeypatch on the application module still steers this
    module.
    """

    store: Any
    logger: Any
    resolve_model: Callable[..., Awaitable[Any]]


# ---------------------------------------------------------------------------
# Pure helpers — no application dependency, so main imports them directly (§P4)
# ---------------------------------------------------------------------------


def provider_of_model(model: str) -> str | None:
    """Sync prefix-based provider heuristic for legacy dispatch paths.

    Catalog rows carry ``provider_ref`` explicitly; this helper exists for
    the small set of code paths that don't have a row in hand and only
    need the factory name (aux-model key injection, vision-model key
    lookup, dispatcher provider-key inference). Returns None on any miss
    so callers fall through to their config_name / env-var heuristics.

    The legacy ``resolve_builtin`` lookup that this replaced was the entry
    point for the YAML fallback path — removed in chunk 6 of the
    models_yaml_removal work.
    """
    if not model:
        return None
    name = model.lower()
    for prefix in ("openrouter/", "groq/"):
        if name.startswith(prefix):
            return prefix.rstrip("/")
    if name.startswith("codex/"):
        return "codex"
    if name.startswith("openai/"):
        return "openai"
    if name.startswith(("claude-",)):
        return "anthropic"
    if name.startswith("gemini-") or name.startswith("gemma-"):
        return "google"
    if name.startswith(("gpt-", "o1", "o3", "o4", "text-embedding-")):
        return "openai"
    return None


def nested_model_slots(
    config_override: dict[str, Any],
) -> list[tuple[str, dict[str, Any], str]]:
    """``(label, section, capability)`` for every NESTED model slot of a
    config_override-shaped dict — the slots the top-level ``llm`` /
    ``auxiliary`` branches of the two credential injectors never look at.

    Since U1 those are ``llm.summarization``, the roster-wide
    ``subagents.llm`` and every roster entry's ``llm`` (+ its own
    ``summarization``); ``llm.strategic`` / ``llm.tactical`` stay for the
    no-blob fallback path, where a pre-U1 job override still carries them and
    the agent lifts model + transport together at its own seam (u1_plan D.5).
    Shared by ``_inject_dispatch_credentials`` (jobs) and
    :func:`inject_thread_dispatch_credentials` (sessions) so the two cannot
    drift. Only mappings are returned; the callers skip a slot with no model
    and the ``inherit`` sentinel.
    """
    out: list[tuple[str, Any, str]] = []
    llm = config_override.get("llm")
    if isinstance(llm, dict):
        for key in ("strategic", "tactical", "summarization"):
            out.append((f"llm.{key}", llm.get(key), "chat"))
    subagents = config_override.get("subagents")
    if isinstance(subagents, dict):
        out.append(("subagents.llm", subagents.get("llm"), "chat"))
        roster = subagents.get("roster")
        if isinstance(roster, dict):
            for name, entry in roster.items():
                if not isinstance(entry, dict):
                    continue
                entry_llm = entry.get("llm")
                out.append((f"subagents.roster.{name}.llm", entry_llm, "chat"))
                if isinstance(entry_llm, dict):
                    out.append(
                        (
                            f"subagents.roster.{name}.llm.summarization",
                            entry_llm.get("summarization"),
                            "chat",
                        )
                    )
    return [(label, sect, cap) for label, sect, cap in out if isinstance(sect, dict)]


def dispatch_llm_provider_fallback(
    job: dict, config_override: dict | None
) -> str | None:
    """Legacy dispatcher provider detection, used only when the model ID
    can't be resolved through the registry.

    Mirrors the pre-registry behavior: explicit ``llm.provider`` wins,
    then the known built-in model catalog, then a config-name heuristic
    (only ``anthropic`` today), finally ``openai``.
    """
    if config_override:
        llm = config_override.get("llm", {})
        if llm.get("provider"):
            return llm["provider"].lower()
        model = llm.get("model")
        if model:
            prov = provider_of_model(model)
            if prov is not None:
                return prov

    config_name = canonical_config_name(job.get("config_name") or "worker_base")
    if config_name and "anthropic" in config_name.lower():
        return "anthropic"
    return "openai"


# ---------------------------------------------------------------------------
# Registry seeding
# ---------------------------------------------------------------------------


async def seed_registry_model_overrides(
    request_override: dict[str, Any] | None,
    *,
    user_id: str | None,
    dependencies: DispatchCredentialDependencies,
) -> dict[str, Any] | None:
    """Seed per-model registry values into the request override BEFORE
    ``resolve_config`` bakes the settings matrix.

    The blob dispatch path freezes ``limits`` (``model_max_context_tokens`` and
    its derived ``context_threshold_tokens`` = ``0.80 × base``) at resolve time,
    and the agent never re-derives them: ``load_config_from_resolved`` ->
    ``load_agent_config_from_dict`` parses the frozen blob without re-running
    ``_apply_settings_matrix``. So an admin's per-model ``context_window``
    (Admin -> Models) must land in ``llm.model_max_context_tokens`` *before*
    ``_apply_settings_matrix`` runs (``config_resolver.resolve_config``), or the
    family default wins and the cap is silently ignored. See
    ``knowledge-base/knowledge/issues/per_model_context_window_override_shadowed_in_blob_dispatch.md``.

    ``resolve_config`` is pure/synchronous (no DB), so the registry lookup
    happens here and rides the existing ``request_override`` layer: its llm keys
    enter ``explicit_llm_keys`` (the matrix won't clobber them) and deep-merge
    into ``data["llm"]`` (becoming the limits-derivation base).

    ``setdefault`` semantics: an explicit caller pin still wins; the family
    default — absent from the bare override pre-resolve — loses. Returns a copy
    with a fresh ``llm`` subdict; the input (the job's persisted
    ``config_override``) is never mutated. The legacy path does not call
    ``resolve_config`` and is unaffected — its ``_inject_dispatch_credentials``
    / :func:`inject_model_credentials` setdefault stays the injection mechanism
    there (and becomes a harmless no-op on the blob path once the value is
    baked). Carries the future per-model ``max_output_tokens`` the same way
    (``[[reasoning_aware_max_output_tokens]]``).
    """
    model_id = ((request_override or {}).get("llm") or {}).get("model")
    if not model_id:
        return request_override
    try:
        meta = await dependencies.resolve_model(model_id, user_id=user_id)
    except UnknownModelError:
        return request_override
    if not meta or not (meta.context_window or meta.max_output_tokens):
        return request_override  # nothing per-model to seed (truthy rejects None/0)
    co = dict(request_override or {})
    llm = dict(co.get("llm") or {})
    if meta.context_window:
        llm.setdefault("model_max_context_tokens", meta.context_window)
    if meta.max_output_tokens:
        # Per-model output cap → enters explicit_llm_keys so the settings matrix
        # won't re-bake the family value, then _resolve_max_output_tokens clamps
        # it to the context backstop. Same shadow-avoiding path as context_window.
        llm.setdefault("max_output_tokens", meta.max_output_tokens)
    co["llm"] = llm
    return co


# ---------------------------------------------------------------------------
# Credential injection
# ---------------------------------------------------------------------------


async def inject_model_credentials(
    *,
    section: dict,
    model_id: str,
    user_id: str | None,
    resolved_keys: dict[str, str] | None,
    capability: str = "chat",
    dependencies: DispatchCredentialDependencies,
) -> None:
    """Populate a config-override section with the right base_url + api_key
    for a given model ID.

    For endpoint-backed models (``origin`` in {``custom``, ``system``}):
    looks up the endpoint row and inlines its ``base_url`` + ``api_key``.
    Custom endpoints are user-scoped; system endpoints are helm-seeded or
    managed via Admin → Providers. Both live in llm_endpoints.

    For built-ins: injects the named provider's key from ``resolved_keys``
    (the user > project > env resolution chain). No base_url injection —
    the agent's own registry handles env-driven base URLs for local models.

    Endpoint-backed models use the endpoint row as the transport authority. That
    intentionally replaces stale persisted transports when a session or paused
    job is rehydrated.
    """
    meta = None
    try:
        meta = await dependencies.resolve_model(
            model_id, user_id=user_id, capability=capability
        )
    except UnknownModelError:
        meta = None

    transport_complete = "base_url" in section and "api_key" in section

    # Per-model context window (chat capability only — auxiliary/vision sections
    # carry their own windows and aren't derived this way). Set before the
    # endpoint/provider branches so it also reaches endpoint-backed (self-hosted)
    # models. setdefault keeps a caller-pinned value; truthy guard skips None/0.
    if capability == "chat" and meta is not None and meta.context_window:
        section.setdefault("model_max_context_tokens", meta.context_window)
    # Per-model output cap (chat slot — strategic/tactical reasoning models are
    # where output truncation bites); overrides the family value, ctx-clamped.
    if capability == "chat" and meta is not None and meta.max_output_tokens:
        section.setdefault("max_output_tokens", meta.max_output_tokens)

    # Inject the agent-side factory name so the section always routes to the
    # correct LLM factory — e.g. an OpenRouter row → _create_openrouter_llm
    # (openrouter.ai), not the OpenAI default at api.openai.com. meta.provider
    # already holds the factory name ("openai" for endpoint-backed rows).
    # This must happen for endpoint rows too: a session hot-swap deep-merges
    # the enriched override into the existing config, so leaving `provider`
    # unset keeps the PREVIOUS model's factory (e.g. minimax via openrouter →
    # gpt-5.5 endpoint row kept routing through _create_openrouter_llm).
    if meta is not None and meta.provider:
        section["provider"] = meta.provider

    # Transport headers the resolved route needs. Only the subscription proxy
    # uses this today: its Claude executor decides thinking visibility from the
    # inbound Anthropic-Beta header, so a Claude-Code-served model without it
    # returns empty thinking blocks (billed, unreadable). Written on every
    # injection — including as `{}` — for the same reason `provider` is: a
    # session hot-swap deep-merges this section over the previous model's, and
    # a header left behind would describe the model that is no longer running.
    # Caller-pinned values still win.
    if meta is not None:
        _route_headers = subscription_request_headers(
            transport_kind=meta.transport_kind,
            subscription_sources=meta.subscription_sources,
        )
        _route_headers.update(section.get("extra_headers") or {})
        # `None`, not `{}`, when nothing applies: the agent-side deep_merge
        # treats None as "clear this field", so a swap away from a Claude
        # account actually drops the header instead of inheriting it. Same
        # sentinel the provider/base_url/api_key swap path uses.
        section["extra_headers"] = _route_headers or None

    if transport_complete and not (meta is not None and meta.endpoint_id):
        return

    if (
        meta is not None
        and meta.origin in ("custom", "system", "catalog")
        and meta.endpoint_id
    ):
        endpoint_row = await dependencies.store.get_user_llm_endpoint(meta.endpoint_id)
        if endpoint_row:
            if endpoint_row.get("base_url"):
                section["base_url"] = endpoint_row["base_url"]
            if endpoint_row.get("api_key"):
                section["api_key"] = endpoint_row["api_key"]
        return

    provider = meta.api_key_ref if meta is not None else provider_of_model(model_id)
    if meta is None and provider:
        section.setdefault("provider", provider)
    # A provider-key row carries no endpoint of its own: the resolved key
    # belongs to the provider's canonical endpoint, not to a ``base_url`` the
    # caller pinned in this section. Withhold the stored key when a caller
    # base_url is present so a system/project/user credential is never sent to
    # a caller-chosen host (the exfiltration in the credential-leak review).
    if provider and resolved_keys and provider in resolved_keys:
        if "api_key" in section:
            return
        if section.get("base_url"):
            dependencies.logger.warning(
                "Dispatch: %s model %r has a pinned base_url; withholding the "
                "resolved %s key so it is not sent to that endpoint.",
                capability,
                model_id,
                provider,
            )
            return
        section["api_key"] = resolved_keys[provider]


async def inject_env_key_credentials(
    *,
    env_keys: dict,
    prefix: str,
    model_id: str,
    user_id: str | None,
    resolved_keys: dict[str, str] | None,
    capability: str = "chat",
    dependencies: DispatchCredentialDependencies,
) -> None:
    """Populate ``env_keys`` with ``{PREFIX}_MODEL/_BASE_URL/_API_KEY``.

    Sibling of :func:`inject_model_credentials` for capabilities that travel as
    flat env vars (vision, whisper, tts, ...) rather than structured config
    sections. Endpoint-backed models (origin in {'custom','system','catalog'}
    with an endpoint_id) contribute the inline base_url+api_key from the
    endpoint row, which overwrites any pre-set endpoint name; built-ins and
    system-anchored catalog rows resolve the api_key via
    ``resolved_keys[provider]`` (setdefault), withheld when an endpoint name
    for the prefix is already set.
    """
    env_keys.setdefault(f"{prefix}_MODEL", model_id)

    meta = None
    try:
        meta = await dependencies.resolve_model(
            model_id, user_id=user_id, capability=capability
        )
    except UnknownModelError:
        meta = None

    if (
        meta is not None
        and meta.origin in ("custom", "system", "catalog")
        and meta.endpoint_id
    ):
        endpoint_row = await dependencies.store.get_user_llm_endpoint(meta.endpoint_id)
        if endpoint_row:
            base_url = endpoint_row.get("base_url")
            api_key = endpoint_row.get("api_key")
            if base_url and not api_key:
                # The endpoint is configured but its stored key didn't decrypt
                # (get_user_llm_endpoint -> _decrypt_stored already logged the
                # cause) or is empty. Surface it loudly and do NOT emit a
                # half-credential (base_url without api_key) that silently
                # degrades the agent to a keyless 'local' provider — the failure
                # mode in knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md.
                dependencies.logger.error(
                    "Dispatch: %s endpoint %s resolved a base_url but no usable "
                    "api_key (decrypt failed or empty) — not injecting "
                    "%s_BASE_URL/_API_KEY; re-add the key in Admin → Models.",
                    prefix,
                    meta.endpoint_id,
                    prefix,
                )
                return
            # The endpoint row is authoritative for its own transport, exactly
            # like the model-section branch: overwrite, never setdefault, so a
            # pre-set endpoint name (under any alias a reader consults) cannot
            # survive next to this row's key.
            if base_url:
                names = env_endpoint_names(prefix)
                for alias in names[1:]:
                    env_keys.pop(alias, None)
                env_keys[names[0]] = base_url
            if api_key:
                env_keys[f"{prefix}_API_KEY"] = api_key
        return

    provider = meta.api_key_ref if meta is not None else provider_of_model(model_id)
    # Same contract as the model-section injector: a provider-key row has no
    # endpoint of its own, so the resolved key belongs to the provider's
    # canonical endpoint. If any endpoint name for this prefix is already set
    # (the endpoint-backed branch above returned for real endpoint rows),
    # withhold the key so a stored credential is not sent to that host.
    if provider and resolved_keys and provider in resolved_keys:
        pinned = [name for name in env_endpoint_names(prefix) if env_keys.get(name)]
        if pinned:
            dependencies.logger.warning(
                "Dispatch: %s model %r has a pinned endpoint (%s); withholding "
                "the resolved %s key so it is not sent to that endpoint.",
                prefix,
                model_id,
                ", ".join(pinned),
                provider,
            )
        else:
            env_keys.setdefault(f"{prefix}_API_KEY", resolved_keys[provider])


async def inject_search_credentials(
    config_override: dict[str, Any],
    *,
    user_settings: dict[str, Any],
    user_id: str | None,
    resolved_keys: dict[str, str],
    dependencies: DispatchCredentialDependencies,
) -> dict[str, Any]:
    """Inject catalog-resolved search/fetch adapters into research config.

    Transport credentials stay in the per-dispatch override and are stripped
    before persistence by ``redact_config_override``. Adapter selection comes
    only from parsed ``params_json`` returned by the shared capability resolver.
    """

    # Function-local on purpose: the shared resolver is the authority
    # (``services/capability_credentials.py``) and the suites steer it by
    # patching the attribute on THAT module. A module-level ``from ... import``
    # here would bind the original at import time and silently defeat the patch.
    from orchestrator.services.capability_credentials import (
        resolve_capability_credentials,
    )

    research = config_override.setdefault("research", {})
    resolved: dict[str, Any] = {}
    for capability in ("search", "fetch"):
        creds = await resolve_capability_credentials(
            capability=capability,
            user_settings=user_settings,
            user_id=user_id,
            resolved_keys=resolved_keys,
            postgres_db=dependencies.store,
        )
        if creds is None:
            research.pop(capability, None)
            continue
        ops_value = creds.params.get("ops")
        ops = (
            [str(op) for op in ops_value if isinstance(op, str)]
            if isinstance(ops_value, list)
            else []
        )
        if not creds.provider or not ops:
            research.pop(capability, None)
            dependencies.logger.warning(
                "Dispatch: %s model %r has no valid params_json.provider/ops; "
                "web tools for that capability are disabled",
                capability,
                creds.model,
            )
            continue

        resolved[capability] = creds
        research[capability] = {
            "provider": creds.provider,
            "base_url": creds.base_url,
            "api_key": creds.api_key,
            "ops": ops,
        }
        dependencies.logger.info(
            "Dispatch: injected %s provider %s (%s)",
            capability,
            creds.provider,
            creds.model,
        )

    primary = resolved.get("search")
    fallback = await resolve_capability_credentials(
        capability="search",
        setting_key="default_search_fallback_model",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=dependencies.store,
    )
    fallback_ops_value = fallback.params.get("ops") if fallback is not None else None
    fallback_ops = (
        [str(op) for op in fallback_ops_value if isinstance(op, str)]
        if isinstance(fallback_ops_value, list)
        else []
    )
    different_row = bool(
        primary is not None
        and fallback is not None
        and primary.catalog_id
        and fallback.catalog_id
        and primary.catalog_id != fallback.catalog_id
    )
    if (
        primary is None
        or fallback is None
        or not different_row
        or not fallback.provider
        or "search" not in fallback_ops
    ):
        research.pop("search_fallback", None)
    else:
        research["search_fallback"] = {
            "provider": fallback.provider,
            "base_url": fallback.base_url,
            "api_key": fallback.api_key,
            "ops": fallback_ops,
        }
        dependencies.logger.info(
            "Dispatch: injected search fallback provider %s (%s)",
            fallback.provider,
            fallback.model,
        )

    if not research:
        config_override.pop("research", None)
    return config_override


async def inject_system_kb_embedding_profile(
    env_keys: dict[str, Any],
    *,
    dependencies: DispatchCredentialDependencies,
) -> str | None:
    """Inject the stable, system-owned embedding profile for knowledge bases.

    The OKF datasource indexer runs in the orchestrator and therefore embeds
    every repository with the admin-curated *system* embedding model. Agent
    memory may instead use a user's embedding preference. Shipping the system
    profile under a separate prefix lets KnowledgeStore query with the exact
    model/transport used by the indexer without changing RecallStore semantics.

    ``KB_EMBEDDING_*`` is intentionally authoritative: callers cannot pin a
    different profile in ``config_override``, and persisted non-secret values
    are refreshed after an administrator changes the system default.
    """
    prefix = "KB_EMBEDDING_"
    for key in [key for key in env_keys if key.startswith(prefix)]:
        del env_keys[key]

    model_id = await dependencies.store.resolve_default_for_capability("embedding")
    if not model_id:
        # Dev/compose compatibility: the central indexer historically used the
        # orchestrator's EMBEDDING_* environment when no catalog pin existed.
        # Materialize that *effective* profile into KB_* too; otherwise the
        # indexer would use this env model while an agent fell back to its
        # per-user model and filtered every indexed chunk out.
        from shared.runtime.services.embedding_service import EmbeddingService

        provider = os.getenv("EMBEDDING_PROVIDER", "local").lower()
        fallback_key = (
            os.getenv("OPENROUTER_API_KEY")
            if provider == "openrouter"
            else os.getenv("EMBEDDING_API_KEY") or os.getenv("OPENAI_API_KEY")
        )
        # The SDK rejects missing credentials during construction, before the
        # old fallback.api_key guard could run. A fresh install has no provider
        # yet: leave the KB sweep idle without constructing a broken client.
        if not fallback_key:
            return None
        fallback = EmbeddingService()
        if not fallback.api_key:
            return None
        env_keys.update(
            {
                "KB_EMBEDDING_PROVIDER": fallback.provider,
                "KB_EMBEDDING_MODEL": fallback.model,
                "KB_EMBEDDING_BASE_URL": fallback.base_url,
                "KB_EMBEDDING_API_KEY": fallback.api_key,
                "KB_EMBEDDING_DIMENSIONS": str(fallback.expected_dimensions),
            }
        )
        return fallback.model

    # Built-in/catalog transports use system_api_keys. Do not reuse the job's
    # already-resolved key map here: it includes project/user overrides and
    # would recreate the same per-user profile skew this path prevents.
    system_keys = await dependencies.store.resolve_api_keys_for_job(
        user_id=None,
        project_id=None,
    )
    await inject_env_key_credentials(
        env_keys=env_keys,
        prefix="KB_EMBEDDING",
        model_id=model_id,
        user_id=None,
        resolved_keys=system_keys,
        capability="embedding",
        dependencies=dependencies,
    )

    try:
        meta = await dependencies.resolve_model(
            model_id, user_id=None, capability="embedding"
        )
    except UnknownModelError:
        meta = None
    env_keys["KB_EMBEDDING_PROVIDER"] = (
        meta.provider if meta is not None and meta.provider else "local"
    )
    if meta is not None:
        # Non-secret registry identity travels with the transport so central
        # indexing and agent-side query filtering derive the same vector stamp.
        # Prefer the concrete endpoint UUID; provider-key catalog rows fall
        # back to their registry/key-reference identity (never the key value).
        profile_anchor = meta.endpoint_id or meta.api_key_ref or meta.model_id
        env_keys["KB_EMBEDDING_PROFILE_ID"] = f"{meta.origin}:{profile_anchor}"
    env_keys["KB_EMBEDDING_DIMENSIONS"] = str(
        os.environ.get("KB_EMBEDDING_DIMENSIONS")
        or os.environ.get("EMBEDDING_DIMENSIONS")
        or "4096"
    )
    return model_id


async def inject_thread_dispatch_credentials(
    config_override: dict[str, Any],
    *,
    user_id: str | None,
    project_id: str | None = None,
    user_settings: dict[str, Any] | None = None,
    include_kb_profile: bool = False,
    dependencies: DispatchCredentialDependencies,
) -> dict[str, Any]:
    """Resolve + inject LLM / auxiliary / embedding credentials into a thread's
    ``config_override`` IN PLACE (creating sections as needed). Returns the dict.

    The persistent-session sibling of the worker-job ``_inject_dispatch_credentials``.
    Secrets travel **in-flight only** — at thread create, and re-injected at session
    attach/resume (the agent workspace endpoint + the resume dispatcher) — and are
    stripped via ``redact_config_override`` before persistence, so
    ``threads.metadata.config_override`` never stores plaintext keys.

    Re-injection-safe: endpoint-backed model transports are refreshed from the
    endpoint row, while provider-key models keep caller-supplied transports.
    :func:`inject_env_key_credentials` is ``setdefault``-based, so running this on
    a stripped copy repopulates the removed secrets without clobbering surviving
    model choices.
    """
    user_settings = user_settings or {}

    # Drop None-valued keys in the model sections before injecting. A prior
    # hot-swap persists explicit ``provider/base_url/api_key = None`` sentinels
    # (they make the live agent's deep_merge CLEAR the previous model's
    # transport); in a stored copy those Nones would block the setdefault-based
    # injection below. Treat them as absent so the transport is repopulated.
    for _sect_name in ("llm", "auxiliary"):
        _sect = config_override.get(_sect_name)
        if isinstance(_sect, dict):
            for _k in [_k for _k, _v in _sect.items() if _v is None]:
                del _sect[_k]

    resolved_keys = await dependencies.store.resolve_api_keys_for_job(
        user_id=user_id,
        project_id=project_id,
    )

    # Chat model. Fall back to the system default chat pin so the agent never
    # boots on its YAML default (which has no transport → api.openai.com 401).
    llm_section = config_override.get("llm") or {}
    if not llm_section.get("model"):
        system_chat_model = await dependencies.store.resolve_default_for_capability(
            "chat"
        )
        if system_chat_model:
            llm_section["model"] = system_chat_model
            dependencies.logger.info(
                "Thread dispatch: injected system default chat model: %s",
                system_chat_model,
            )
    if llm_section.get("model"):
        await inject_model_credentials(
            section=llm_section,
            model_id=llm_section["model"],
            user_id=user_id,
            resolved_keys=resolved_keys,
            dependencies=dependencies,
        )
        config_override["llm"] = llm_section

    # Auxiliary slot (title generation, memory extraction, knowledge curation).
    aux_section = config_override.get("auxiliary") or {}
    if not aux_section.get("model"):
        aux_model = user_settings.get("default_auxiliary_model")
        if not aux_model:
            aux_model = await dependencies.store.resolve_default_for_capability(
                "auxiliary"
            )
        if aux_model:
            aux_section["model"] = aux_model
            dependencies.logger.info(
                "Thread dispatch: injected auxiliary model: %s", aux_model
            )
    if aux_section.get("model"):
        await inject_model_credentials(
            section=aux_section,
            model_id=aux_section["model"],
            user_id=user_id,
            resolved_keys=resolved_keys,
            capability="auxiliary",
            dependencies=dependencies,
        )
        config_override["auxiliary"] = aux_section

    # Nested model slots (U1): `llm.summarization`, the roster-wide
    # `subagents.llm` and every roster entry's `llm` — the same slots and the
    # same helper as the job injector, so a session's roster children reach
    # their endpoints too. None sentinels are stripped per slot for the same
    # reason as the top-level sections above; an inheriting entry carries its
    # parent's model NAME and is routed by it, the bare `inherit` sentinel is
    # not a model.
    for _label, _section, _capability in nested_model_slots(config_override):
        for _k in [_k for _k, _v in _section.items() if _v is None]:
            del _section[_k]
        _model = _section.get("model")
        if not _model or _model == INHERIT_MODEL:
            continue
        await inject_model_credentials(
            section=_section,
            model_id=_model,
            user_id=user_id,
            resolved_keys=resolved_keys,
            capability=_capability,
            dependencies=dependencies,
        )
        dependencies.logger.info(
            "Thread dispatch: injected credentials for %s: %s", _label, _model
        )

    # Embedding capability travels as flat env vars. Source provider/model from
    # the (possibly stripped) persisted block first so re-injection on resume is
    # stable, then user settings, then the system default. Unconditionally call
    # the env-key injector when a model is known: it is setdefault-based, so it
    # re-adds the stripped EMBEDDING_API_KEY without clobbering surviving
    # EMBEDDING_MODEL / EMBEDDING_BASE_URL.
    env_keys_block = config_override.setdefault("env_keys", {})
    embedding_provider = env_keys_block.get("EMBEDDING_PROVIDER") or user_settings.get(
        "embedding_provider"
    )
    embedding_model = env_keys_block.get("EMBEDDING_MODEL") or user_settings.get(
        "default_embedding_model"
    )
    if not embedding_model:
        embedding_model = await dependencies.store.resolve_default_for_capability(
            "embedding"
        )
    if embedding_provider:
        env_keys_block.setdefault("EMBEDDING_PROVIDER", embedding_provider)
    if embedding_model:
        await inject_env_key_credentials(
            env_keys=env_keys_block,
            prefix="EMBEDDING",
            model_id=embedding_model,
            user_id=user_id,
            resolved_keys=resolved_keys,
            capability="embedding",
            dependencies=dependencies,
        )
    if (
        embedding_provider == "openrouter"
        and resolved_keys
        and "openrouter" in resolved_keys
    ):
        env_keys_block.setdefault("OPENROUTER_API_KEY", resolved_keys["openrouter"])
    # The memory reranker travels the same way (RERANK_MODEL / _BASE_URL /
    # _API_KEY): the persisted block first so re-injection on resume is
    # stable, then the user's pin, then the admin default for the ``rerank``
    # capability. The agent rides the embedding transport when no RERANK_*
    # keys arrive (single-router deployments), so this block is additive.
    rerank_model = env_keys_block.get("RERANK_MODEL") or user_settings.get(
        "default_rerank_model"
    )
    if not rerank_model:
        rerank_model = await dependencies.store.resolve_default_for_capability("rerank")
    if rerank_model:
        await inject_env_key_credentials(
            env_keys=env_keys_block,
            prefix="RERANK",
            model_id=rerank_model,
            user_id=user_id,
            resolved_keys=resolved_keys,
            capability="rerank",
            dependencies=dependencies,
        )
    if include_kb_profile:
        await inject_system_kb_embedding_profile(
            env_keys_block, dependencies=dependencies
        )
    else:
        for key in [key for key in env_keys_block if key.startswith("KB_EMBEDDING_")]:
            del env_keys_block[key]
    if not env_keys_block:
        config_override.pop("env_keys", None)

    await inject_search_credentials(
        config_override,
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        dependencies=dependencies,
    )

    return config_override


__all__ = [
    "DispatchCredentialDependencies",
    "dispatch_llm_provider_fallback",
    "inject_env_key_credentials",
    "inject_model_credentials",
    "inject_search_credentials",
    "inject_system_kb_embedding_profile",
    "inject_thread_dispatch_credentials",
    "nested_model_slots",
    "provider_of_model",
    "seed_registry_model_overrides",
]
