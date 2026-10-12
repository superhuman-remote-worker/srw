"""Readiness gate for the LLM stack.

Today an admin can configure a provider key, skip Admin → Models entirely,
create a session, and hit a 503/401 on first turn because no chat-capability
catalog row exists. This module computes the readiness signal that:

- Powers the cockpit's onboarding checklist (provider → models → model defaults
  pinned → application expert defaults selected).
- Gates ``POST /api/jobs`` and ``POST /api/persistent/threads`` with a
  503 when a required capability is missing.

It also owns the auto-pin (:func:`auto_pin_required_defaults`): a required
capability that has enabled catalog rows but no default pin gets one, so a
fresh install that adds one model per capability is ready without a trip to
Admin → Models → Defaults.

Required capabilities: ``chat``, ``embedding``, ``auxiliary``, ``rerank``.
Optional: ``vision`` (falls back to chat when
``llm.fallback_optional_capabilities_to_chat`` is true), ``whisper``, ``tts``
(audio features disable when missing).

Rerank is required for the same reason embedding is: the memory pipeline binds
the reranker scorer unconditionally (``scorers: [reranker]`` in
``config/expert_base.yaml``) and a configured scorer is required — a session or
job whose ``/rerank`` route is unreachable fails every turn, so a fresh install
must not be able to start one. Embedding and rerank are slated to become
optional together (grep-based knowledge navigation already works without
vectors); until then the gate holds both.

Auxiliary is required (not optional + chat-fallback) because the
auxiliary LLM runs the memory observer and knowledge curator on a
separate task budget; defaulting it to chat means every observer pass
competes with the live agent for chat-model tokens, defeats per-capability
rate-limiting, and tends to surface as "the agent feels slower" without
an obvious cause.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from shared.helm_provenance import AUTO_PIN_BREADCRUMB, SOURCE_DEFAULT

logger = logging.getLogger(__name__)

# Capabilities that *must* be ready before the cockpit releases. Aligns
# with knowledge-base/knowledge/features/models_yaml_removal.md §"Role-completeness gate".
REQUIRED_CAPABILITIES = ("chat", "embedding", "auxiliary", "rerank")

# Optional capabilities — surfaced in the readiness payload so the cockpit
# can show "vision is missing → falls back to chat" hints, but they don't
# block the gate.
OPTIONAL_CAPABILITIES = ("vision", "whisper", "tts")
REQUIRED_EXPERT_DEFAULTS = ("worker", "session")

# When ``llm.fallback_optional_capabilities_to_chat`` is true (the default),
# missing ``vision`` resolves to the configured chat model and audio
# features simply disable; when false, the optional capabilities also
# gate the cockpit (operators who want strict capability separation).
DEFAULT_FALLBACK_OPTIONAL_TO_CHAT = True


async def compute_readiness(db: Any) -> dict[str, Any]:
    """Build the readiness payload consumed by the cockpit + dispatcher.

    Returns:
        ``{ready, missing_providers, missing_capabilities, missing_defaults,
        missing_expert_defaults, optional_capability_fallbacks}``.

        - ``ready`` is False iff any required capability is missing rows, a
          model default pin, or an application expert default.
        - ``missing_providers`` is non-empty only when *no* provider is
          configured at all (the existing onboarding gate's signal).
        - ``optional_capability_fallbacks`` carries the per-capability
          fallback target so the cockpit can render the right hint.
    """
    api_keys = await db.list_system_api_keys()
    endpoints = await db.list_system_llm_endpoints()
    # Search/fetch providers share the endpoint table with model transports.
    # Use catalog capabilities rather than labels (operators can rename them).
    # A newly added endpoint without catalog rows still completes this first
    # setup step; model capabilities/defaults are checked separately below.
    research_only: set[str] = set()
    model_endpoints: set[str] = set()
    if endpoints and not api_keys:
        for model in await db.list_models(provider_kind="endpoint"):
            ref = str(model["provider_ref"])
            caps = set(model.get("capabilities") or [])
            if caps and caps <= {"search", "fetch"}:
                research_only.add(ref)
            else:
                model_endpoints.add(ref)
    research_only -= model_endpoints
    has_any_provider = bool(api_keys) or any(
        str(endpoint["id"]) not in research_only for endpoint in endpoints
    )

    counts = await db.count_enabled_models_by_capability()
    pinned_caps = set(await db.list_default_pin_capabilities())

    missing_capabilities = [
        cap for cap in REQUIRED_CAPABILITIES if counts.get(cap, 0) <= 0
    ]
    # A default pin is required only when at least one row exists for the
    # capability; pinning into a thin air doesn't help anyone.
    missing_defaults = [
        cap
        for cap in REQUIRED_CAPABILITIES
        if counts.get(cap, 0) > 0 and cap not in pinned_caps
    ]

    fallback_to_chat = await _fallback_optional_capabilities_to_chat(db)
    optional_fallbacks: dict[str, str | None] = {}
    for cap in OPTIONAL_CAPABILITIES:
        if counts.get(cap, 0) > 0:
            optional_fallbacks[cap] = None  # natively available
        elif cap == "vision" and fallback_to_chat:
            optional_fallbacks[cap] = "use_chat"
        else:
            optional_fallbacks[cap] = None  # disabled (no fallback)

    missing_providers: list[str] = [] if has_any_provider else ["any"]

    missing_expert_defaults: list[str] = []
    if os.getenv("EXPERTS_DB_ENABLED", "true").lower().strip() in (
        "true",
        "1",
        "yes",
    ):
        # The query joins the pointer to its expert row, so a missing/corrupt
        # target is reported the same way as an absent pointer. Startup seeds
        # both slots; this check catches runtime/operator drift afterward.
        expert_defaults = await db.list_application_expert_defaults()
        configured_types = {
            str(row.get("expert_type") or row.get("default_type"))
            for row in expert_defaults
        }
        missing_expert_defaults = [
            expert_type
            for expert_type in REQUIRED_EXPERT_DEFAULTS
            if expert_type not in configured_types
        ]

    ready = (
        has_any_provider
        and not missing_capabilities
        and not missing_defaults
        and not missing_expert_defaults
    )

    return {
        "ready": ready,
        "missing_providers": missing_providers,
        "missing_capabilities": missing_capabilities,
        "missing_defaults": missing_defaults,
        "missing_expert_defaults": missing_expert_defaults,
        "optional_capability_fallbacks": optional_fallbacks,
    }


async def auto_pin_required_defaults(db: Any) -> list[tuple[str, str]]:
    """Pin a default for each required capability that has rows but no pin.

    The pinned row is the one dispatch already falls back to when no pin
    exists (the first enabled row by ``display_label``, see
    ``PostgresDB.resolve_default_for_capability``), so auto-pinning changes
    no runtime behaviour: it only makes the choice visible in Admin → Models
    → Defaults and satisfies the readiness gate. In the usual one-model-at-a-
    time setup that is simply the first model added for the capability.

    Never replaces a pin: a capability that already names a model is skipped,
    and the write itself is conditional, so an admin pin racing this call
    wins. Auto pins carry ``updated_by=AUTO_PIN_BREADCRUMB`` so a declared
    ``llm.seed.defaults`` entry can still replace them.

    Returns the ``(capability, model_id)`` pairs it pinned.
    """
    pinned = set(await db.list_default_pin_capabilities())
    pinned_now: list[tuple[str, str]] = []
    for capability in REQUIRED_CAPABILITIES:
        if capability in pinned:
            continue
        candidates = await db.list_models_by_capability_alphabetical(capability)
        if not candidates:
            continue
        model_id = candidates[0]["model_id"]
        if await db.pin_default_llm_model_if_unset(
            capability,
            model_id,
            updated_by=AUTO_PIN_BREADCRUMB,
            source=SOURCE_DEFAULT,
        ):
            logger.info(
                "auto-pinned default %s model to %s (no pin was set)",
                capability,
                model_id,
            )
            pinned_now.append((capability, model_id))
    return pinned_now


async def try_auto_pin_required_defaults(db: Any) -> list[tuple[str, str]]:
    """:func:`auto_pin_required_defaults` for write paths that must not fail.

    Called after catalog writes (admin API, seed Job, subscription import,
    startup); a failure is logged and leaves the capability for the admin to
    pin, which is exactly the pre-auto-pin behaviour.
    """
    try:
        return await auto_pin_required_defaults(db)
    except Exception:
        logger.warning("auto-pinning required default models failed", exc_info=True)
        return []


async def _fallback_optional_capabilities_to_chat(db: Any) -> bool:
    """Read the ``llm.fallback_optional_capabilities_to_chat`` system flag.

    Default ``True`` (pragmatic — operators who want strict separation
    flip the flag).
    """
    row = await db.get_system_setting("llm.fallback_optional_capabilities_to_chat")
    if row is None:
        return DEFAULT_FALLBACK_OPTIONAL_TO_CHAT
    value = row.get("value")
    if isinstance(value, dict):
        return bool(value.get("enabled", DEFAULT_FALLBACK_OPTIONAL_TO_CHAT))
    if isinstance(value, bool):
        return value
    return DEFAULT_FALLBACK_OPTIONAL_TO_CHAT


def gate_error_detail(readiness: dict[str, Any]) -> dict[str, Any]:
    """Shape the 503 error body for ``POST /api/jobs`` / ``POST /api/persistent/threads``.

    Carries the same ``missing_*`` fields the cockpit reads from
    ``/api/system/readiness`` so the UI can deep-link to the right admin
    page from either source.
    """
    return {
        "error": "system_not_ready",
        "missing_providers": readiness.get("missing_providers", []),
        "missing_capabilities": readiness.get("missing_capabilities", []),
        "missing_defaults": readiness.get("missing_defaults", []),
        "missing_expert_defaults": readiness.get("missing_expert_defaults", []),
        "message": _build_message(readiness),
    }


def _build_message(readiness: dict[str, Any]) -> str:
    parts: list[str] = []
    if readiness.get("missing_providers"):
        parts.append("Configure at least one provider key or endpoint")
    if readiness.get("missing_capabilities"):
        caps = ", ".join(readiness["missing_capabilities"])
        parts.append(f"add a model row for: {caps}")
    if readiness.get("missing_defaults"):
        caps = ", ".join(readiness["missing_defaults"])
        parts.append(f"pin a default model for: {caps}")
    if readiness.get("missing_expert_defaults"):
        expert_types = ", ".join(readiness["missing_expert_defaults"])
        parts.append(f"select an application default expert for: {expert_types}")
    if not parts:
        return "System ready."
    return (
        "System not ready — "
        + "; ".join(parts)
        + ". Visit the relevant Admin settings to finish setup."
    )
