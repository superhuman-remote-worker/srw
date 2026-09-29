"""Bridge nested session-create overrides into the validated rebuild.

``POST /api/persistent/threads`` rebuilds ``config_override`` from validated
fragments: it starts from ``{}`` and copies in the pieces it names. Anything it
does not name is dropped — silently, until this module. The New Session form
lifts ``model`` and ``permission_mode`` to top-level request fields but leaves
``reasoning_level`` and ``temperature`` NESTED under ``config_override.llm``,
which is exactly the shape the rebuild never read. Five earlier fields fell
into the same hole (permission mode, workspace backend, eight tool groups, the
officer block, then reasoning). See
knowledge-base/knowledge/issues/live_settings_silently_dropped_on_stateless_sessions.md
§Defect B.

Two pure helpers, so the contract is unit-testable without the create handler:

- :func:`bridge_nested_llm_override` folds the nested LLM keys into the rebuilt
  override. A top-level request field always wins over its nested twin.
- :func:`ignored_override_paths` is the warn phase of a strict contract
  (Kubernetes KEP-2885 shape: Ignore → Warn → Strict): every nested path whose
  value the rebuild did not carry, so callers can log and surface it before a
  later change turns the list into a 400.

The request-fragment VALIDATORS below were moved verbatim from
``orchestrator.main`` (R1.B05 lane P, census group ``R_SESSION_POLICY``). They
belong to the same rebuild: ``create_thread`` starts from ``{}`` and copies in
only what these functions return, so a fragment nobody validates here is a
fragment silently dropped. Their shared discipline is that garbage fails LOUD
with ``HTTPException(400)`` rather than disappearing — an unknown officer key,
a non-integer bound or a reasoning level outside the vocabulary is a 400, not a
silent drop.

Three refusals in that group are authorization, not shape, and are load-bearing:
``officer.slots.*.spend_ceiling_daily``, ``auto_pull`` and
``worker_spend_ceiling_daily`` authorize unattended work or bound money spend,
are owned by the durable Officer Post, and reach a runtime ONLY through the
server-private snapshot seam :func:`validated_post_owned_officer_create_fragment`
reads. That function additionally materializes safe ABSENT values, so an
account or expert default cannot re-introduce the authority while a commission
request resolves, and :func:`effective_officer_post_owned_refusal` names the
field an untrusted create inherited.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import HTTPException

#: LLM keys the create handler honours, top-level or nested.
LLM_KEYS: tuple[str, ...] = ("model", "temperature", "reasoning_level")


class SessionOverrideError(ValueError):
    """A nested override key is present but its value is malformed."""


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def bridge_nested_llm_override(
    request_override: dict[str, Any] | None,
    config_override: dict[str, Any],
    *,
    validate_reasoning_level: Callable[[Any], str],
) -> list[str]:
    """Fold ``request_override["llm"][k]`` for ``k in LLM_KEYS`` into
    ``config_override["llm"]`` where the rebuilt override has no value yet.

    Mutates ``config_override`` in place and returns the bridged key paths
    (``["llm.reasoning_level", ...]``) for the caller's audit line. A key the
    caller already bridged from a top-level request field is left alone: the
    explicit field is the stronger statement of intent.

    Raises :class:`SessionOverrideError` for a malformed nested value.
    ``validate_reasoning_level`` is the create handler's own vocabulary check
    and may raise its own error type (an ``HTTPException`` today); it is
    called only for a nested level that is actually being bridged.
    """
    nested_llm = _as_dict(_as_dict(request_override).get("llm"))
    if not nested_llm:
        return []
    target = config_override.setdefault("llm", {})
    if not isinstance(target, dict):
        return []
    bridged: list[str] = []
    for key in LLM_KEYS:
        if key not in nested_llm or key in target:
            continue
        value = nested_llm[key]
        if value is None:
            continue
        if key == "model":
            if not isinstance(value, str) or not value.strip():
                raise SessionOverrideError(
                    "config_override.llm.model must be a non-empty string"
                )
            target[key] = value
        elif key == "temperature":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SessionOverrideError(
                    "config_override.llm.temperature must be a number"
                )
            target[key] = value
        else:
            target[key] = validate_reasoning_level(value)
        bridged.append(f"llm.{key}")
    return bridged


#: The ``delegation`` keys a session request may set: the live keys after U3
#: (loader.normalize_delegation_block) plus the session's own fan-out cap
#: (parallel_subagents.md D2: overridable per expert and per session). The
#: fan-out gates ``session_fanout`` / ``session_fanout_pinned`` are operator
#: and expert configuration and stay refused here.
DELEGATION_KEYS: frozenset[str] = frozenset(
    {
        "enabled",
        "max_concurrent",
        "run_in_background_default",
        "session_max_concurrent",
    }
)
#: How many children one session turn runs at once (D2): 1 to 20.
SESSION_MAX_CONCURRENT_RANGE: tuple[int, int] = (1, 20)
#: Pre-U3 keys the loader drops with a deprecation warning; tolerated here for
#: the same reason (a stored layer may still carry them), never persisted. One
#: source of truth: the loader's own list, so the two boundaries cannot drift.
try:
    from shared.runtime.core.loader import _LEGACY_DELEGATION_KEYS as _LOADER_LEGACY

    LEGACY_DELEGATION_KEYS: frozenset[str] = frozenset(_LOADER_LEGACY)
except ImportError:  # pragma: no cover - the loader is always importable here
    LEGACY_DELEGATION_KEYS = frozenset(
        {
            "max_depth",
            "default_timeout",
            "max_timeout",
            "allowed_configs",
            "mode",
            "light",
        }
    )


def validate_delegation_override(value: Any) -> dict[str, Any]:
    """The ``delegation`` block a session request may carry, validated.

    ``delegate_agent`` and its control plane are ``grant: explicit``: the
    factory builds them only when ``tools.delegation`` names them AND
    ``delegation.enabled`` is true. The Cockpit's Delegation toggle therefore
    writes BOTH (tools-group.component.ts ``getOverrides``), and this is the
    boundary that must carry the second half — the create rebuild dropped it,
    so a ticked "Delegation" produced five names the agent then refused to
    bind ("11 configured tool(s) did not bind"). Malformed values are a
    :class:`SessionOverrideError`, never a silent drop.
    """
    if not isinstance(value, dict):
        raise SessionOverrideError("config_override.delegation must be an object")
    out: dict[str, Any] = {}
    for key, raw in value.items():
        if key in LEGACY_DELEGATION_KEYS:
            continue
        if key not in DELEGATION_KEYS:
            raise SessionOverrideError(
                f"config_override.delegation.{key} is not a session delegation setting"
            )
        if key in ("enabled", "run_in_background_default"):
            if not isinstance(raw, bool):
                raise SessionOverrideError(
                    f"config_override.delegation.{key} must be a boolean"
                )
            out[key] = raw
        elif key == "session_max_concurrent":
            low, high = SESSION_MAX_CONCURRENT_RANGE
            if (
                isinstance(raw, bool)
                or not isinstance(raw, int)
                or not low <= raw <= high
            ):
                raise SessionOverrideError(
                    "config_override.delegation.session_max_concurrent must be "
                    f"an integer from {low} to {high}"
                )
            out[key] = raw
        else:
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
                raise SessionOverrideError(
                    "config_override.delegation.max_concurrent must be a positive integer"
                )
            out[key] = raw
    return out


def bridge_nested_delegation_override(
    request_override: dict[str, Any] | None,
    config_override: dict[str, Any],
) -> list[str]:
    """Fold a validated ``request_override["delegation"]`` into the rebuilt
    override. Returns the bridged key paths; ``[]`` when nothing was sent."""
    raw = _as_dict(request_override).get("delegation")
    if raw is None:
        return []
    validated = validate_delegation_override(raw)
    if not validated:
        return []
    target = config_override.setdefault("delegation", {})
    if not isinstance(target, dict):
        return []
    bridged: list[str] = []
    for key, val in validated.items():
        if key in target:
            continue
        target[key] = val
        bridged.append(f"delegation.{key}")
    return bridged


def ignored_override_paths(
    request_override: dict[str, Any] | None,
    config_override: dict[str, Any],
) -> list[str]:
    """Key paths the caller sent under ``config_override`` that the rebuilt
    override does not carry with the same value.

    Compared by VALUE, not by allow-list, so a key that arrived twice (the
    form sends ``model`` top-level AND nested) is not reported when the
    rebuild carries it. One level of nesting is inspected — the sections the
    create handler validates (``llm``, ``interactive``, ``workspace``,
    ``tools``, ``officer``) are all flat dicts of scalars or lists — and an
    unknown top-level section is reported as a whole.

    Sorted for a stable audit line.
    """
    request = _as_dict(request_override)
    ignored: list[str] = []
    for section, sent in request.items():
        carried = config_override.get(section)
        if isinstance(sent, dict):
            carried_dict = carried if isinstance(carried, dict) else {}
            for key, value in sent.items():
                if key not in carried_dict or carried_dict[key] != value:
                    ignored.append(f"{section}.{key}")
        elif carried != sent:
            ignored.append(section)
    return sorted(ignored)


# Reasoning-effort vocabulary accepted at session create. The superset across
# families — the family capability clamps to what the chosen model actually
# supports at attach (loader._clamp_reasoning_level), so over-asking degrades
# gracefully; garbage fails loud here instead of being silently dropped.
SESSION_REASONING_LEVELS = frozenset({"none", "low", "medium", "high", "xhigh", "max"})


def validated_reasoning_level(value: Any) -> str:
    level = str(value or "").strip().lower()
    if level not in SESSION_REASONING_LEVELS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"reasoning_level must be one of {sorted(SESSION_REASONING_LEVELS)}"
            ),
        )
    return level


SESSION_OFFICER_OVERRIDE_KEYS = frozenset(
    {
        "enabled",
        "sleep_min_minutes",
        "sleep_max_minutes",
        "max_concurrent_workers",
        "max_actions_per_wake",
        "daily_token_ceiling",
        "slots",
        "conference",
    }
)

# These values authorize unattended work or bound its money spend. They are
# owned by the durable Officer Post and must never be accepted from the generic
# session-create/config surfaces. Explicit commission carries them through the
# non-model-selectable ``_officer_post_config_snapshot`` seam below.
OFFICER_POST_OWNED_CREATE_KEYS = frozenset(
    {"auto_pull", "worker_spend_ceiling_daily", "slots"}
)


def validated_session_officer_override(
    config_override: Any,
) -> Optional[dict[str, Any]]:
    """Extract + validate the ``officer`` sub-dict from a New Session request's
    ``config_override`` (centurion.md §4/§8).

    The officer flag MUST land in thread metadata — the orchestrator's officer
    machinery (watchdog, wake-drain claim, sweeper exemptions, the wake-filing
    endpoint's 409 gate) is SQL over ``threads.metadata`` and cannot see
    resolved expert config. ``create_thread`` rebuilds ``config_override``
    from validated fragments only, so without this validator the officer block
    is silently dropped (found by the S1 k3d smoke). Admits exactly the known
    officer keys: ``enabled`` coerced to a real bool, everything else
    non-negative ints. Auto-pull and spend authority are deliberately absent:
    they are Post-owned and reach a commissioned runtime only through the
    server-private snapshot seam. Raises HTTPException(400) on unknown keys or
    bad types.
    """
    officer = (
        config_override.get("officer") if isinstance(config_override, dict) else None
    )
    if not isinstance(officer, dict) or not officer:
        return None
    unknown = set(officer) - SESSION_OFFICER_OVERRIDE_KEYS
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown officer override keys: {sorted(unknown)}",
        )
    cleaned: dict[str, Any] = {}
    if "enabled" in officer:
        cleaned["enabled"] = officer["enabled"] in (True, "true", "True", 1)
    if "conference" in officer:
        # Conference embodiment (centurion.md §2/S9): identity attachment
        # (charter injection) without officer lifecycle — enabled stays false
        # on conference threads, so the watchdog/drain never touch them.
        cleaned["conference"] = officer["conference"] in (True, "true", "True", 1)
    if "slots" in officer:
        raw_slots = officer["slots"]
        if isinstance(raw_slots, dict) and any(
            isinstance(spec, dict) and "spend_ceiling_daily" in spec
            for spec in raw_slots.values()
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "officer slot spend ceilings are owned by the Officer Post; "
                    "use the project Officer endpoint"
                ),
            )
        # Typed worker roster (officer_slots.py). Validated hard at provision
        # so a typo'd kit fails HERE with a 400, not silently at the
        # officer's first dispatch.
        from orchestrator.services.officer_slots import validate_slots_spec

        try:
            cleaned["slots"] = validate_slots_spec(officer["slots"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    for key in sorted(
        SESSION_OFFICER_OVERRIDE_KEYS
        - {
            "enabled",
            "slots",
            "conference",
        }
    ):
        if key in officer:
            try:
                cleaned[key] = max(0, int(officer[key]))
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"officer.{key} must be an integer",
                ) from exc
    return cleaned or None


def validated_post_owned_officer_create_fragment(
    snapshot: Any,
    *,
    validated_officer_post_patch: Callable[[Any], tuple[dict[str, Any], Any, Any]],
) -> dict[str, Any] | None:
    """Derive the Post-owned runtime fields from a server-private snapshot.

    ``ThreadCreateRequest._officer_post_config_snapshot`` is a Pydantic
    ``PrivateAttr`` and therefore cannot be populated by JSON or by a model.
    Explicit commission sets it only after the owner/release checks and after
    the durable Post update.  Materialize safe absent values as well so an
    account/expert default cannot re-introduce unattended or spend authority
    while the commission request is resolved.
    """
    if snapshot is None:
        return None
    if not isinstance(snapshot, dict):
        raise RuntimeError("Officer Post config snapshot must be an object")
    officer = snapshot.get("officer") or {}
    if not isinstance(officer, dict):
        raise HTTPException(status_code=400, detail="Officer Post config is malformed")

    post_body = {
        key: officer[key] for key in OFFICER_POST_OWNED_CREATE_KEYS if key in officer
    }
    fragment, _policy, _effects = validated_officer_post_patch(post_body)
    cleaned = dict(fragment.get("officer") or {})
    cleaned.setdefault("auto_pull", False)
    cleaned.setdefault("worker_spend_ceiling_daily", None)
    cleaned.setdefault("slots", None)
    return cleaned


def effective_officer_post_owned_refusal(effective_config: Any) -> str | None:
    """Return the Post-owned field inherited by an untrusted Officer create."""
    if not isinstance(effective_config, dict):
        return None
    officer = effective_config.get("officer")
    if not isinstance(officer, dict):
        return None
    if "auto_pull" in officer:
        auto_pull = officer.get("auto_pull")
        if type(auto_pull) is not bool or auto_pull:
            return "auto_pull"
    if officer.get("worker_spend_ceiling_daily") is not None:
        return "worker_spend_ceiling_daily"
    slots = officer.get("slots")
    if isinstance(slots, dict) and any(
        isinstance(spec, dict) and "spend_ceiling_daily" in spec
        for spec in slots.values()
    ):
        return "slots.*.spend_ceiling_daily"
    return None
