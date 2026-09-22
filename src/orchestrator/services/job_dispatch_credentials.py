"""Compose a worker job's credential-complete configuration override.

Extracted verbatim from ``orchestrator.main`` (R1.B05 lane J, census group
``R_DISPATCH``). This module owns the *composition* only: the individual
injectors — model slots, env-key slots, search credentials, the system KB
embedding profile, the provider fallback and the nested roster slot walk — are
lane C's and arrive as injected callables. Credential storage stays with
``services.provider_credentials`` / ``services.capability_credentials``.

Two properties this composition must keep:

* **Order.** The top-level model branch runs before the nested slot loop, the
  user-preference block before the system defaults, and the search credentials
  last. Each later block uses ``setdefault`` against what an earlier one wrote,
  so reordering silently changes which credential wins.
* **Names, never values.** Every log line here names a provider, a model, a
  capability or an env-key *name*. No branch logs a key, and none is added.

The KB embedding profile is the one block that is *removed* when not requested:
``include_kb_profile=False`` deletes every ``KB_EMBEDDING_*`` key, so a
re-dispatch cannot leave a previous scope's profile behind.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from shared.runtime.core.loader import INHERIT_MODEL
from shared.runtime.core.model_registry import UnknownModelError
from shared.subscription_routing import subscription_request_headers


class DispatchCredentialStore(Protocol):
    async def resolve_api_keys_for_job(
        self, *, user_id: str | None, project_id: str | None
    ) -> dict[str, Any]: ...

    async def get_user_settings(self, user_id: str) -> dict[str, Any] | None: ...

    async def get_user_llm_endpoint(
        self, endpoint_id: str
    ) -> dict[str, Any] | None: ...

    async def resolve_default_for_capability(self, capability: str) -> str | None: ...


@dataclass(frozen=True)
class DispatchCredentialDependencies:
    """Per-invocation collaborators for worker credential composition."""

    store: DispatchCredentialStore
    logger: logging.Logger
    resolve_model: Callable[..., Awaitable[Any]]
    inject_model_credentials: Callable[..., Awaitable[Any]]
    inject_env_key_credentials: Callable[..., Awaitable[Any]]
    inject_search_credentials: Callable[..., Awaitable[Any]]
    inject_system_kb_embedding_profile: Callable[
        [dict[str, Any]], Awaitable[str | None]
    ]
    dispatch_llm_provider_fallback: Callable[
        [dict[str, Any], dict[str, Any] | None], str | None
    ]
    nested_model_slots: Callable[[dict[str, Any]], list[tuple[str, Any, str]]]


async def inject_dispatch_credentials(
    job: dict[str, Any],
    config_override: dict[str, Any] | None,
    *,
    include_kb_profile: bool = False,
    dependencies: DispatchCredentialDependencies,
) -> dict[str, Any]:
    """Resolve and inject API keys, model routing, and capability defaults.

    Mutates ``config_override`` in place (creating it if None) with everything
    the agent needs to reach its configured LLM endpoints: per-user/project
    API keys, endpoint base_url + api_key for catalog-routed models, user-
    preference fallbacks (default chat/auxiliary/strategic/tactical models,
    autonomy, reasoning level, vision/whisper/tts), and the system-level
    default chat model when no override pinned one.

    Called from both first-dispatch (``_dispatch_job_to_agent``) and resume
    (``_resume_job_on_agent``) — without this on resume, an orphaned/paused
    job re-dispatched to a fresh agent would inherit only the bare
    creation-time config_override (no model/api_key) and the agent would
    silently fall back to ``OPENAI_API_KEY=not-needed``, producing 401s
    against the user's router.

    Returns the (mutated) ``config_override`` dict so callers can rebind
    locals when the input was None.
    """
    postgres_db = dependencies.store
    logger = dependencies.logger

    job_id = str(job["id"])
    user_id_str = str(job["user_id"]) if job.get("user_id") else None
    project_id_str = str(job["project_id"]) if job.get("project_id") else None

    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id_str,
        project_id=project_id_str,
    )
    user_settings: dict[str, Any] = {}
    if user_id_str:
        user_settings = await postgres_db.get_user_settings(user_id_str) or {}

    config_override = config_override or {}
    llm_over = config_override.setdefault("llm", {})
    model_id = llm_over.get("model")
    meta = None
    if model_id:
        try:
            meta = await dependencies.resolve_model(model_id, user_id=user_id_str)
        except UnknownModelError:
            meta = None

    if (
        meta is not None
        and meta.origin in ("custom", "system", "catalog")
        and meta.endpoint_id
    ):
        # Endpoint-backed models carry their agent-side factory in meta.provider
        # (``openai`` for the OpenAI-compatible wire, ``codex`` for the Responses-
        # API Codex proxy). Inject it so the agent builds the right factory — the
        # endpoint branch otherwise leaves provider unset and the agent defaults
        # to the openai factory, which forces Chat Completions and strips gpt-5.x
        # reasoning.
        if meta.provider:
            llm_over["provider"] = meta.provider
        endpoint_row = await postgres_db.get_user_llm_endpoint(meta.endpoint_id)
        if endpoint_row:
            if endpoint_row.get("base_url"):
                llm_over["base_url"] = endpoint_row["base_url"]
            if endpoint_row.get("api_key"):
                llm_over["api_key"] = endpoint_row["api_key"]
            logger.info(
                f"Dispatch: routed {model_id} to {meta.origin} endpoint "
                f"{endpoint_row.get('label') or meta.endpoint_id}"
            )
    elif resolved_keys:
        if meta is not None and meta.api_key_ref:
            provider_for_key: str | None = meta.api_key_ref
        else:
            provider_for_key = dependencies.dispatch_llm_provider_fallback(
                job, config_override
            )
        # Route to the right agent-side LLM factory. System-anchored catalog
        # rows carry no endpoint base_url, so without an explicit provider the
        # agent's create_llm defaults to the OpenAI factory (api.openai.com)
        # and rejects e.g. an OpenRouter sk-or-v1 key. meta.provider already
        # holds the factory name (_factory_provider); fall back to the
        # key-inference result for registry misses.
        factory_provider = meta.provider if meta is not None else provider_for_key
        if factory_provider:
            llm_over.setdefault("provider", factory_provider)
        # A provider-key row has no endpoint of its own, so the resolved key
        # belongs to that provider's canonical endpoint — never to a base_url
        # the caller pinned in the override. Injecting a system/project/user
        # key next to a caller-chosen ``base_url`` would ship the deployment's
        # credential to whatever host the caller named. REST admission refuses
        # a caller transport key up front (routers/job_lifecycle), but hold the
        # sink to the same contract for a project-override base_url and the
        # resume / legacy / blob paths: skip the injection when the section
        # carries a base_url this branch did not set.
        if (
            provider_for_key
            and provider_for_key in resolved_keys
            and "api_key" not in llm_over
            and "base_url" not in llm_over
        ):
            llm_over["api_key"] = resolved_keys[provider_for_key]
        elif (
            provider_for_key
            and provider_for_key in resolved_keys
            and "api_key" not in llm_over
            and "base_url" in llm_over
        ):
            logger.warning(
                "Dispatch: job %s pinned a base_url with a provider-key model "
                "(%s); withholding the resolved %s key so a stored credential "
                "is not sent to a caller-chosen endpoint.",
                job_id,
                model_id,
                provider_for_key,
            )

    # Per-model context window: drive the agent's working window from the
    # catalog/admin value. Lands in llm.model_max_context_tokens (a flat llm
    # key) so it survives the agent-side settings-matrix re-run and becomes the
    # base for the derived limits. Truthy guard rejects None and an explicit 0.
    if meta is not None and meta.context_window:
        llm_over.setdefault("model_max_context_tokens", meta.context_window)
    # Per-model output cap (params_json): flat llm key, survives the agent-side
    # settings-matrix re-run and overrides the family settings.max_output_tokens.
    if meta is not None and meta.max_output_tokens:
        llm_over.setdefault("max_output_tokens", meta.max_output_tokens)
    # Route transport headers — same contract as the nested slots below
    # (`inject_model_credentials`), restated here because the top-level model
    # is credentialed by this branch rather than by that helper. Written on
    # every dispatch, `{}` included, so a re-dispatch after a model change
    # cannot leave the previous route's headers behind.
    if meta is not None:
        _llm_headers = subscription_request_headers(
            transport_kind=meta.transport_kind,
            subscription_sources=meta.subscription_sources,
        )
        _llm_headers.update(llm_over.get("extra_headers") or {})
        llm_over["extra_headers"] = _llm_headers or None

    if resolved_keys:
        _ENV_KEY_MAP = {"vision": "VISION_API_KEY"}
        env_keys = {
            _ENV_KEY_MAP[p]: resolved_keys[p] for p in ("vision",) if p in resolved_keys
        }
        if env_keys:
            config_override.setdefault("env_keys", {}).update(env_keys)
        logger.info(
            f"Dispatch: injected API keys for providers: {list(resolved_keys.keys())}"
        )

    # Resolve credentials for every OTHER slot the job pinned a model on. The
    # top-level branch above only inspects `llm.model`; without this loop a
    # pinned `auxiliary`, `llm.summarization`, roster-wide `subagents.llm` or
    # roster entry `subagents.roster.<n>.llm` ships the model name with no
    # `base_url`/`api_key`, the agent's LLM factory falls back to the parent's
    # base_url, and the model's endpoint never gets hit — opaque 404s when it
    # lives behind a non-default endpoint (the 2026-05-12 tactical-pin
    # incident). A roster entry that inherits its parent's model carries the
    # parent's model NAME here (the resolver copied it), so it is routed by
    # that name exactly like the top level; the bare `inherit` sentinel is not
    # a model. The legacy `llm.strategic`/`llm.tactical` blocks are kept for
    # the no-blob fallback path only (the blob path lifts them into llm.model
    # before injection).
    _sections: list[tuple[str, Any, str]] = [
        ("auxiliary", config_override.get("auxiliary"), "auxiliary")
    ]
    _sections.extend(dependencies.nested_model_slots(config_override))
    for _section_name, _section, _capability in _sections:
        if not isinstance(_section, dict):
            continue
        _section_model = _section.get("model")
        if not _section_model or _section_model == INHERIT_MODEL:
            continue
        await dependencies.inject_model_credentials(
            section=_section,
            model_id=_section_model,
            user_id=user_id_str,
            resolved_keys=resolved_keys,
            capability=_capability,
        )
        if "api_key" not in _section and "base_url" not in _section:
            logger.warning(
                f"Dispatch: job {job_id} pinned {_section_name} model "
                f"{_section_model!r} but no endpoint or provider key was "
                f"resolvable — the agent will fall back to the parent "
                f"base_url and almost certainly 404."
            )
        else:
            logger.info(
                f"Dispatch: injected credentials for {_section_name} "
                f"override: {_section_model}"
            )

    if job.get("user_id"):
        aux_model = user_settings.get("default_auxiliary_model")
        if not aux_model:
            aux_model = await postgres_db.resolve_default_for_capability("auxiliary")
        if aux_model:
            aux_override = config_override.setdefault("auxiliary", {})
            if "model" not in aux_override:
                aux_override["model"] = aux_model
                await dependencies.inject_model_credentials(
                    section=aux_override,
                    model_id=aux_model,
                    user_id=user_id_str,
                    resolved_keys=resolved_keys,
                    capability="auxiliary",
                )
                logger.info(f"Dispatch: injected auxiliary model override: {aux_model}")

        default_model = user_settings.get("default_model")
        if default_model:
            llm_override = config_override.setdefault("llm", {})
            if "model" not in llm_override:
                llm_override["model"] = default_model
                await dependencies.inject_model_credentials(
                    section=llm_override,
                    model_id=default_model,
                    user_id=user_id_str,
                    resolved_keys=resolved_keys,
                )
                logger.info(f"Dispatch: injected user default_model: {default_model}")

        # Per-phase account model defaults (default_strategic_model /
        # default_tactical_model) were removed: the single top-level
        # default_model above is the only account-level model preference. They
        # silently shadowed an explicit per-loop/per-job top-level model (a phase
        # pin beats the top-level in LLMConfig.get_phase_config) and were
        # invisible/unmanageable in the UI. Explicit per-job phase pins still
        # arrive via config_override.llm.{strategic,tactical} (the request
        # override), credentialed by the inject_model_credentials calls earlier
        # in this function. See
        # knowledge-base/knowledge/issues/loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md (Layer 1).

        default_autonomy = user_settings.get("default_autonomy")
        if default_autonomy and "autonomy" not in config_override:
            config_override["autonomy"] = default_autonomy
            logger.info(f"Dispatch: injected user default_autonomy: {default_autonomy}")

        default_reasoning = user_settings.get("default_reasoning_level")
        if default_reasoning:
            llm_override = config_override.setdefault("llm", {})
            if "reasoning_level" not in llm_override:
                llm_override["reasoning_level"] = default_reasoning
                logger.info(
                    f"Dispatch: injected user default_reasoning_level: {default_reasoning}"
                )

        for _kind, _prefix, _user_key, _capability in (
            ("vision", "VISION", "default_vision_model", "vision"),
            ("whisper", "WHISPER", "default_whisper_model", "whisper"),
            ("tts", "TTS", "default_tts_model", "tts"),
            # Memory reranker: RERANK_MODEL/_BASE_URL/_API_KEY from the `rerank`
            # catalog pin. The agent rides the embedding transport when these
            # are absent (single-router deployments), so this is additive.
            ("rerank", "RERANK", "default_rerank_model", "rerank"),
            ("citation", "CITATION_LLM", "default_citation_model", "chat"),
        ):
            _model = user_settings.get(_user_key)
            if not _model:
                _model = await postgres_db.resolve_default_for_capability(_kind)
            if not _model:
                continue
            env_keys_block = config_override.setdefault("env_keys", {})
            if f"{_prefix}_MODEL" in env_keys_block:
                continue
            await dependencies.inject_env_key_credentials(
                env_keys=env_keys_block,
                prefix=_prefix,
                model_id=_model,
                user_id=user_id_str,
                resolved_keys=resolved_keys,
                capability=_capability,
            )
            # The citation_engine package reads CITATION_LLM_URL (not _BASE_URL)
            # and falls back to OPENAI_API_KEY for auth. Alias the URL key here
            # so the upstream package picks up the dispatched endpoint.
            if _kind == "citation" and "CITATION_LLM_BASE_URL" in env_keys_block:
                env_keys_block.setdefault(
                    "CITATION_LLM_URL", env_keys_block["CITATION_LLM_BASE_URL"]
                )
            logger.info(f"Dispatch: injected {_kind} model: {_model}")

        embedding_provider = user_settings.get("embedding_provider")
        embedding_model = user_settings.get("default_embedding_model")
        if not embedding_model:
            embedding_model = await postgres_db.resolve_default_for_capability(
                "embedding"
            )
        if embedding_provider or embedding_model:
            env_keys_block = config_override.setdefault("env_keys", {})
            if embedding_provider and "EMBEDDING_PROVIDER" not in env_keys_block:
                env_keys_block["EMBEDDING_PROVIDER"] = embedding_provider
            if embedding_model:
                # Resolve endpoint base_url + api_key even when EMBEDDING_MODEL
                # was already set — inject_env_key_credentials uses setdefault,
                # so a pre-present MODEL must not suppress the _API_KEY (the bug
                # that left jobs with MODEL+BASE_URL but no key:
                # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md).
                await dependencies.inject_env_key_credentials(
                    env_keys=env_keys_block,
                    prefix="EMBEDDING",
                    model_id=embedding_model,
                    user_id=user_id_str,
                    resolved_keys=resolved_keys,
                    capability="embedding",
                )
            if (
                embedding_provider == "openrouter"
                and resolved_keys
                and "openrouter" in resolved_keys
            ):
                env_keys_block["OPENROUTER_API_KEY"] = resolved_keys["openrouter"]
            logger.info(
                f"Dispatch: injected embedding: "
                f"provider={embedding_provider}, model={embedding_model}"
            )

    # System-default fallback for the worker chat model. Runs after the
    # user-preference block (or whenever there's no user) so jobs that
    # arrived without an llm.model still pick up the admin-curated default
    # from the catalog instead of falling through to the agent's YAML
    # default — which has no base_url/api_key for self-hosted models and
    # silently routes to api.openai.com with "not-needed".
    llm_override_check = config_override.get("llm") or {}
    if "model" not in llm_override_check:
        system_chat_model = await postgres_db.resolve_default_for_capability("chat")
        if system_chat_model:
            llm_override = config_override.setdefault("llm", {})
            llm_override["model"] = system_chat_model
            await dependencies.inject_model_credentials(
                section=llm_override,
                model_id=system_chat_model,
                user_id=user_id_str,
                resolved_keys=resolved_keys,
            )
            logger.info(
                f"Dispatch: injected system default chat model: {system_chat_model} "
                f"(job {job_id})"
            )

    # System-default fallback for the embedding credential — same rationale as
    # the chat model above. Embedding (memory + KB) was otherwise resolved ONLY
    # inside the user-preference block, so a job whose user has no embedding
    # preference (or no user at all) silently fell back to provider 'local' with
    # no key, disabling memory + KB with no signal. Inject the admin-curated
    # system embedding here so every job gets it the same way it gets its chat
    # model. knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
    _emb_env = config_override.setdefault("env_keys", {})
    if "EMBEDDING_API_KEY" not in _emb_env:
        _emb_model = _emb_env.get(
            "EMBEDDING_MODEL"
        ) or await postgres_db.resolve_default_for_capability("embedding")
        if _emb_model:
            await dependencies.inject_env_key_credentials(
                env_keys=_emb_env,
                prefix="EMBEDDING",
                model_id=_emb_model,
                user_id=user_id_str,
                resolved_keys=resolved_keys,
                capability="embedding",
            )
            if _emb_env.get("EMBEDDING_API_KEY"):
                logger.info(
                    f"Dispatch: injected system default embedding: {_emb_model} "
                    f"(job {job_id})"
                )
            else:
                logger.error(
                    f"Dispatch: system embedding model {_emb_model!r} resolved no "
                    f"usable API key for job {job_id} — memory/KB will be "
                    f"unavailable. Check the embedding endpoint (Admin → Models)."
                )

    if include_kb_profile:
        _kb_emb_model = await dependencies.inject_system_kb_embedding_profile(_emb_env)
        if _kb_emb_model:
            if _emb_env.get("KB_EMBEDDING_API_KEY"):
                logger.info(
                    "Dispatch: injected system OKF KB embedding profile: %s (job %s)",
                    _kb_emb_model,
                    job_id,
                )
            else:
                logger.error(
                    "Dispatch: system OKF KB embedding model %r resolved no usable "
                    "API key for job %s; knowledge retrieval will be unavailable.",
                    _kb_emb_model,
                    job_id,
                )
    else:
        for key in [key for key in _emb_env if key.startswith("KB_EMBEDDING_")]:
            del _emb_env[key]

    await dependencies.inject_search_credentials(
        config_override,
        user_settings=user_settings,
        user_id=user_id_str,
        resolved_keys=resolved_keys,
    )

    return config_override
