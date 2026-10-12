"""Approved-user account preferences and their HTTP validation contract."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import re
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, field_validator

from orchestrator.security.auth import require_approved_user
from orchestrator.services.model_availability import (
    MODEL_UNAVAILABLE_CODE,
    WHERE_ACCOUNT,
    UnavailableModel,
    render_unavailable_message,
    stale_account_model_reason,
)
from orchestrator.services.preference_defaults import resolve_preference_defaults

router = APIRouter(prefix="/api/settings/preferences")

# The account model preferences checked against the registry
# (unavailable_model_handling.md §7): (path, slot, capability).
_MODEL_PREFERENCES = (
    (("default_model",), "llm", "chat"),
    (("default_auxiliary_model",), "auxiliary", "auxiliary"),
    (("persistent_agent", "model"), "llm", "chat"),
)


def _preference_value(settings: Mapping[str, Any] | None, path: tuple[str, ...]) -> Any:
    value: Any = settings or {}
    for key in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


@dataclass
class PreferencesDependencies:
    """App-owned persistence, identity and configuration inputs."""

    db: Any
    role_base: Callable[[str], dict[str, Any]]
    environ: Mapping[str, str]
    require_approved_user: Callable[[Request, Any], Awaitable[dict[str, Any]]] = (
        require_approved_user
    )


def get_preferences_dependencies(request: Request) -> PreferencesDependencies:
    return request.app.state.preferences_dependencies


class UserSettingsUpdate(BaseModel):
    """Request body for updating user preferences. Null values remove the key."""

    default_model: str | None = None
    default_autonomy: str | None = None
    default_reasoning_level: str | None = None
    default_auxiliary_model: str | None = None
    default_vision_model: str | None = None
    default_whisper_model: str | None = None
    default_tts_model: str | None = None
    default_search_model: str | None = None
    default_fetch_model: str | None = None
    default_search_fallback_model: str | None = None
    default_tts_voice: str | None = None
    # NOTE: per-phase model defaults (default_strategic_model /
    # default_tactical_model) were removed — see Layer 1 in
    # knowledge-base/knowledge/issues/loop_ran_codex_spark_not_selected_model_then_hung_on_cooldown.md.
    # ``default_chat_model`` and ``default_session_model`` were removed for a
    # duller reason: nothing ever read them. The account-level chat model is
    # ``default_model``, and a session's is ``persistent_agent.model``.
    # Old clients PATCHing any of them are ignored (BaseModel drops unknown
    # fields) — but a PATCH carrying ONLY a dropped key now 400s as "No
    # settings provided", which is the honest answer.
    default_embedding_model: str | None = None
    embedding_provider: str | None = None
    # Admin "View as" preference: 'all' = fleet-wide visibility (default),
    # 'me' = shadow regular-user visibility. Read by the cockpit's
    # ViewModeService; the live request narrowing rides the X-Admin-View-As
    # header (orchestrator/security/auth.py), this just persists the choice.
    admin_view_mode: Literal["me", "all"] | None = None
    # persistent_agent sub-object: model, permission_mode,
    # idle_timeout_minutes, headless_mode and headless_attention_sleep_minutes.
    # Patch-replaces the whole sub-object. Free-form by design, so legacy keys
    # from removed controls (greeting, command_allowlist,
    # notification_channels) still round-trip harmlessly if a stored blob
    # carries them — nothing reads them any more. The retired workspace_backend
    # is the exception: it is dropped (see _validate_persistent_agent).
    persistent_agent: dict[str, Any] | None = None
    # Read-aloud rewrite preferences: {reasoning_level, custom_prompt}. Controls
    # how the auxiliary LLM rewrites a message for speech — reasoning_level (off
    # by default, keeps the fast path) and a free-text custom_prompt (the user's
    # standing style/summarization instructions). Read by services/tts.py's
    # rewrite path. Patch-replaces the whole sub-object.
    read_aloud: dict[str, Any] | None = None
    # Notification/communication preferences: {delivery, channels, quiet_hours}.
    # Read by services/notification_service.py (_get_user_channels,
    # _is_in_quiet_hours) and by the agent-message delivery path. This field was
    # missing until 2026-08-23: the cockpit's Communication card PATCHed
    # ``communication`` into a model that did not declare it, Pydantic dropped it
    # (extra="ignore"), the request 400'd as "No settings provided", and no user
    # ever had the key. Patch-replaces the whole sub-object.
    communication: dict[str, Any] | None = None
    # Cockpit UI locale (BCP-47, e.g. "en" / "de-DE"). Client-only — nothing
    # server-side reads it; the cockpit stores it here and reads it back from
    # GET /api/settings/preferences. Kept as a free string rather than an enum
    # so shipping a new locale is a cockpit-only change; I18nService.toSupported
    # already normalises unknown tags to its default on read. Undeclared until
    # 2026-08-23, so language choice never persisted for anyone.
    language: str | None = None

    @field_validator("persistent_agent")
    @classmethod
    def _validate_persistent_agent(
        cls, v: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        """Guard the persistent_agent sub-object. ``workspace_backend`` is a
        retired preference (Slice A2b: a Session's tier now comes only from
        the workspace defaults chain) — accepted from a stale cockpit and
        silently dropped, rather than 422ing, so an old client's PATCH still
        succeeds for its other fields. Other keys stay free-form (Phase 6
        contract)."""
        if v is None:
            return v
        if not isinstance(v, dict):
            raise ValueError("persistent_agent must be an object")
        v = {key: value for key, value in v.items() if key != "workspace_backend"}
        return v

    @field_validator("read_aloud")
    @classmethod
    def _validate_read_aloud(cls, v: dict[str, Any] | None) -> dict[str, Any] | None:
        """Guard the read-aloud sub-object: reasoning_level must be one of the
        allowed levels, and custom_prompt is length-capped (it rides on every aux
        rewrite call). Mirrors the server-side constants in services/tts.py."""
        if v is None:
            return v
        if not isinstance(v, dict):
            raise ValueError("read_aloud must be an object")
        from orchestrator.services.tts import (
            READ_ALOUD_PROMPT_MAX,
            READ_ALOUD_REASONING_LEVELS,
        )

        level = v.get("reasoning_level")
        if level is not None:
            if (
                not isinstance(level, str)
                or level.lower() not in READ_ALOUD_REASONING_LEVELS
            ):
                raise ValueError(
                    f"reasoning_level must be one of {list(READ_ALOUD_REASONING_LEVELS)}"
                )
            v["reasoning_level"] = level.lower()
        prompt = v.get("custom_prompt")
        if prompt is not None:
            if not isinstance(prompt, str):
                raise ValueError("custom_prompt must be a string")
            if len(prompt) > READ_ALOUD_PROMPT_MAX:
                raise ValueError(
                    f"custom_prompt must be at most {READ_ALOUD_PROMPT_MAX} characters"
                )
        return v

    @field_validator("communication")
    @classmethod
    def _validate_communication(cls, v: dict[str, Any] | None) -> dict[str, Any] | None:
        """Guard the communication sub-object. ``channels`` values must be real
        booleans: every reader gates on ``channels.get(name, True)``, so a string
        ``"false"`` would be truthy and silently leave a channel switched on —
        the exact failure the user would be trying to fix. The three known
        sub-objects must be objects; other keys stay free-form, matching the
        persistent_agent contract."""
        if v is None:
            return v
        if not isinstance(v, dict):
            raise ValueError("communication must be an object")
        for key in ("delivery", "channels", "quiet_hours"):
            sub = v.get(key)
            if sub is not None and not isinstance(sub, dict):
                raise ValueError(f"communication.{key} must be an object")
        for name, enabled in (v.get("channels") or {}).items():
            if not isinstance(enabled, bool):
                raise ValueError(
                    f"communication.channels.{name} must be a boolean, "
                    f"got {type(enabled).__name__}"
                )
        # D9 preference matrix: categories[category][channel] = bool overrides
        # the channel-type default above. Same strict-bool rule, same reason.
        categories = v.get("categories")
        if categories is not None:
            if not isinstance(categories, dict):
                raise ValueError("communication.categories must be an object")
            for category, cells in categories.items():
                if not isinstance(cells, dict):
                    raise ValueError(
                        f"communication.categories.{category} must be an object"
                    )
                for channel, enabled in cells.items():
                    if not isinstance(enabled, bool):
                        raise ValueError(
                            f"communication.categories.{category}.{channel} must be "
                            f"a boolean, got {type(enabled).__name__}"
                        )
        # How long a `normal` notification waits for someone to look before it
        # mails, when no project officer owns the wait. Minutes; bounded so a
        # typo cannot mean "never" or "instantly".
        minutes = v.get("escalation_minutes")
        if minutes is not None:
            from orchestrator.services.notification_catalog import (
                ESCALATION_MINUTES_BOUNDS,
            )

            lo, hi = ESCALATION_MINUTES_BOUNDS
            if isinstance(minutes, bool) or not isinstance(minutes, int):
                raise ValueError("communication.escalation_minutes must be an integer")
            if not lo <= minutes <= hi:
                raise ValueError(
                    f"communication.escalation_minutes must be between {lo} and {hi}"
                )
        return v

    @field_validator("language")
    @classmethod
    def _validate_language(cls, v: str | None) -> str | None:
        """Sanity-cap the locale tag. Deliberately not an enum — see the field
        comment. Just enough to keep junk out of the settings JSONB."""
        if v is None:
            return v
        if not isinstance(v, str) or not re.fullmatch(
            r"[A-Za-z]{2,8}(-[A-Za-z0-9]{1,8})*", v
        ):
            raise ValueError("language must be a BCP-47 tag such as 'en' or 'de-DE'")
        return v


@router.get("")
async def get_user_preferences(
    request: Request,
    *,
    dependencies: PreferencesDependencies = Depends(get_preferences_dependencies),
) -> dict[str, Any]:
    """Get the current user's preference settings.

    The response includes a ``_resolved`` key containing the effective
    default for every preference field (derived from framework YAML configs
    and environment variables). The UI uses this to display the actual
    value behind "Server default" / "Not set".
    """
    user = await dependencies.require_approved_user(request, dependencies.db)
    prefs = await dependencies.db.get_user_settings(str(user["id"]))
    # persistent_agent.workspace_backend is a retired preference (Slice A2b):
    # hide it on read even though a stored row may still carry it. The startup
    # backfill copies it into the personal project and leaves it in place; the
    # next preferences PATCH that replaces persistent_agent drops it.
    if isinstance(prefs.get("persistent_agent"), dict):
        prefs["persistent_agent"] = {
            key: value
            for key, value in prefs["persistent_agent"].items()
            if key != "workspace_backend"
        }
    prefs["_resolved"] = await resolve_preference_defaults(
        dependencies.db, role_base=dependencies.role_base, environ=dependencies.environ
    )
    # A stored model preference that can no longer run, keyed by its dotted
    # path, so the page can show it as unavailable instead of a blank select.
    unavailable: dict[str, str] = {}
    for path, _slot, capability in _MODEL_PREFERENCES:
        model = _preference_value(prefs, path)
        if not model or not isinstance(model, str):
            continue
        reason = await stale_account_model_reason(
            model, user_id=str(user["id"]), capability=capability, store=dependencies.db
        )
        if reason is not None:
            unavailable[".".join(path)] = reason
    prefs["_unavailable"] = unavailable
    return prefs


@router.patch("")
async def update_user_preferences(
    request: Request,
    body: UserSettingsUpdate,
    *,
    dependencies: PreferencesDependencies = Depends(get_preferences_dependencies),
) -> dict[str, str]:
    """Update the current user's preference settings (patch-merge)."""
    user = await dependencies.require_approved_user(request, dependencies.db)
    settings = {
        k: v
        for k, v in body.model_dump().items()
        if v is not None or k in body.model_fields_set
    }
    if not settings:
        raise HTTPException(status_code=400, detail="No settings provided")
    await _refuse_unavailable_model_preferences(user, settings, dependencies)
    await dependencies.db.update_user_settings(str(user["id"]), settings)
    return {"status": "updated"}


async def _refuse_unavailable_model_preferences(
    user: Mapping[str, Any],
    settings: Mapping[str, Any],
    dependencies: PreferencesDependencies,
) -> None:
    """Refuse saving a model preference that cannot run (400, the model named).

    Only a value that differs from the stored one is checked: the page saves
    every field together, and an already-stale stored default must not block
    an unrelated change. Clearing (``null``) is always allowed.
    """
    candidates = [
        (path, slot, capability, model)
        for path, slot, capability in _MODEL_PREFERENCES
        if path[0] in settings
        and isinstance(model := _preference_value(settings, path), str)
        and model
    ]
    stored: Mapping[str, Any] | None = None
    entries: list[UnavailableModel] = []
    for path, slot, capability, model in candidates:
        reason = await stale_account_model_reason(
            model, user_id=str(user["id"]), capability=capability, store=dependencies.db
        )
        if reason is None:
            continue
        if stored is None:
            stored = await dependencies.db.get_user_settings(str(user["id"])) or {}
        if model == _preference_value(stored, path):
            continue
        entries.append(UnavailableModel(slot=slot, model=model, reason=reason))
    if entries:
        raise HTTPException(
            status_code=400,
            detail={
                "code": MODEL_UNAVAILABLE_CODE,
                "message": render_unavailable_message(
                    entries,
                    where=WHERE_ACCOUNT,
                    is_admin=bool(user.get("is_admin")),
                ),
                "entries": [entry.as_dict() for entry in entries],
            },
        )
