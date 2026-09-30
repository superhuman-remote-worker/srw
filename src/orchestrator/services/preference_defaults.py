"""Resolve account preference defaults using app-supplied configuration."""

from collections.abc import Callable, Mapping
from typing import Any


async def resolve_preference_defaults(
    db: Any,
    *,
    role_base: Callable[[str], dict[str, Any]],
    environ: Mapping[str, str],
) -> dict[str, Any]:
    """Compute resolved default values for all user preference fields.

    The chat/auxiliary/session model defaults come from the DB model registry
    (``resolve_default_for_capability`` — the SAME source dispatch uses), so the
    UI shows the model the agent will actually run, not the worker base's
    placeholder. Non-model fields (autonomy, reasoning, helper-model env
    fallbacks) still read framework defaults / env vars. This lets the UI show
    the actual effective value instead of "Not set" / "Server default".
    """
    # The two role bases, fully merged (expert_base + overlay): `autonomy`
    # lives in the worker overlay and `llm.model` in expert_base, so neither
    # file alone answers.
    worker_cfg = role_base("worker")
    persistent_cfg = role_base("session")

    llm = worker_cfg.get("llm", {})
    aux = worker_cfg.get("auxiliary", {})
    p_llm = persistent_cfg.get("llm", {})

    # System chat/auxiliary defaults come from the DB model registry — the same
    # source dispatch resolves via resolve_default_for_capability — NOT the YAML
    # placeholder, so the displayed "default" is the model the agent will run.
    # Fall back to the YAML model only when the registry has no capability default.
    registry_chat = await db.resolve_default_for_capability("chat")
    registry_aux = await db.resolve_default_for_capability("auxiliary")
    # Helper models resolve the way dispatch resolves them (the user's pick, then
    # this registry default — job_dispatch_credentials / resolve_capability_
    # credentials), so Settings shows the model actually in effect: TTS for the
    # voice picker's voice list, vision/whisper/embedding for the agent. The env
    # var is only a last-ditch fallback; with neither, the default is None
    # ("nothing configured") rather than a model name the catalog may not hold.
    registry_vision = await db.resolve_default_for_capability("vision")
    registry_whisper = await db.resolve_default_for_capability("whisper")
    registry_tts = await db.resolve_default_for_capability("tts")
    registry_embedding = await db.resolve_default_for_capability("embedding")

    return {
        "default_model": registry_chat or llm.get("model"),
        "default_autonomy": worker_cfg.get("autonomy"),
        "default_reasoning_level": llm.get("reasoning_level"),
        "default_auxiliary_model": registry_aux or aux.get("model") or llm.get("model"),
        "default_vision_model": registry_vision or environ.get("VISION_MODEL"),
        "default_whisper_model": registry_whisper or environ.get("WHISPER_MODEL"),
        "default_tts_model": registry_tts or environ.get("TTS_MODEL"),
        "default_embedding_model": (
            registry_embedding or environ.get("EMBEDDING_MODEL")
        ),
        # Not a model: mirrors embedding_service's provider default.
        "embedding_provider": environ.get("EMBEDDING_PROVIDER", "local"),
        # Admin "View as" default — fleet-wide visibility unless the admin
        # has explicitly narrowed to their own data.
        "admin_view_mode": "all",
        "persistent_agent": {
            # Sessions resolve their base model via the same chat-capability
            # default (base_defaults in _resolve_session_config), so surface that
            # — not the session base's placeholder.
            "model": registry_chat or p_llm.get("model"),
            "permission_mode": "supervised",
            "idle_timeout_minutes": 30,
        },
    }
