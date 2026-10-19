"""Officer Post editing policy: the patch validator and its vocabulary.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; officer_post.md §7). This module is deliberately pure — it makes
no store call and takes no side effect — because a typo'd kit must fail *here*,
on the same hard line the session-create funnel draws, and that rule has to be
testable without a database.

What travels with it:

* :data:`OFFICER_POST_EFFECTS` — §7's per-field honesty table, shown in the UI.
  It is also the accepted-field vocabulary: anything outside it is a 400.
* :func:`validated_officer_post_patch` — one partial edit (PATCH body or
  commission body — same shape) → ``(config_fragment, communication_policy
  patch, effects)``. Null-as-clear is preserved per field, and
  ``communication_policy: null`` is still refused with its explanation.
* :func:`enforce_officer_auto_pull_release` — the deployment release fence for
  unattended enablement. Callers pass the *effective* commissioned value, so a
  post enabled before a rollback cannot be recommissioned behind a dark fence;
  the historical truthy shapes (``"true"``, ``1``) are fenced too.
* :func:`check_officer_sleep_bounds` — min ≤ max over the MERGED view.
* :data:`OFFICER_CONFIG_NAME` / :data:`OFFICER_PERMISSION_MODE` — the two
  commission-time facts whose absence each cost a live incarnation its whole
  job surface (2026-08-15); their comments are the record of that.
* :func:`format_legate_note` and :data:`OFFICER_NOTE_MAX_CHARS` — the Legate
  channel's authorship stamp and bound.

The release fence is the one runtime value, and it arrives as a **callable** on
:class:`OfficerPostPolicyDependencies` because it is a B11-owned import-time
flag that suites rebind on ``orchestrator.main`` (§P1).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from fastapi import HTTPException

from orchestrator.services.session_create_overrides import validated_reasoning_level


@dataclass
class OfficerPostPolicyDependencies:
    """The deployment release fence, read live."""

    auto_pull_release_enabled: Callable[[], bool]


OFFICER_CONFIG_NAME = "centurion"

# A commissioned officer runs headless — no session exists in which a human
# could answer a permission prompt — so he must not inherit the create
# endpoint's ``read_only`` default. A post may still pin a stricter mode
# explicitly; this only fills the absent case.
OFFICER_PERMISSION_MODE = "autonomous"

OFFICER_POST_EFFECTS: dict[str, str] = {
    "slots": "next dispatch",
    "auto_pull": "next dispatch",
    "worker_spend_ceiling_daily": "next dispatch",
    "max_concurrent_workers": "next dispatch",
    "daily_token_ceiling": "next delivery",
    "sleep_min_minutes": "next sleep filing + watchdog immediately",
    "sleep_max_minutes": "next sleep filing + watchdog immediately",
    "max_actions_per_wake": "next respawn",
    "brain": "next respawn",
    "communication_policy": "next worker message",
}

OFFICER_POST_INT_FIELDS = frozenset(
    {
        "max_concurrent_workers",
        "max_actions_per_wake",
        "daily_token_ceiling",
        "sleep_min_minutes",
        "sleep_max_minutes",
    }
)

OFFICER_POST_POSITIVE_NUMBER_FIELDS = frozenset({"worker_spend_ceiling_daily"})

# Row-only worker-message routing policy (officer_post.md §7): the server
# resolves it per message, the officer thread can never rewrite it, and it is
# deliberately NEVER mirrored into thread metadata.
COMMUNICATION_WORKER_MESSAGES = frozenset(
    {"user_direct", "officer_and_user", "officer_first"}
)
COMMUNICATION_RESPONSE_MINUTES_BOUNDS = (5, 120)


def validated_officer_post_patch(
    body: Any,
) -> tuple[dict[str, Any], Optional[dict[str, Any]], dict[str, str]]:
    """Validate a partial post edit (PATCH body, commission body — same shape).

    Returns ``(config_fragment, communication_policy_patch, effects)`` where
    the fragment is row/thread-mergeable (``{"officer": {...}, "llm":
    {...}}``), the policy patch stays row-only, and ``effects`` carries §7's
    per-field labels for exactly the keys the caller sent. Raises
    HTTPException(400) on unknown fields or bad values — a typo'd kit fails
    HERE, the same hard line the create funnel draws
    (``_validated_session_officer_override``).

    Null-as-clear (the card's revert affordance): an explicit ``null`` on
    ``slots`` (→ flat cap), ``brain`` (→ session default), or any numeric
    field (→ platform default) writes a JSON null through the deep merge —
    every reader treats null as unset. ``communication_policy: null`` is
    refused with an explanation; the policy always has explicit values.
    """
    if body is None:
        return {}, None, {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Body must be a JSON object")
    unknown = set(body) - set(OFFICER_POST_EFFECTS)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown officer post fields: {sorted(unknown)}",
        )

    officer_patch: dict[str, Any] = {}
    llm_patch: dict[str, Any] = {}
    comm_patch: Optional[dict[str, Any]] = None

    if "slots" in body:
        if body["slots"] is None:
            officer_patch["slots"] = None  # revert to flat cap
        else:
            # The same hard validation provision gets — a typo'd kit 400s.
            from orchestrator.services.officer_slots import validate_slots_spec

            try:
                officer_patch["slots"] = validate_slots_spec(body["slots"])
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    if "auto_pull" in body:
        value = body["auto_pull"]
        if value is None:
            # Null clears to the safe database/UI default rather than leaving
            # a truthy historical projection behind.
            officer_patch["auto_pull"] = False
        elif not isinstance(value, bool):
            raise HTTPException(status_code=400, detail="auto_pull must be a boolean")
        else:
            officer_patch["auto_pull"] = value

    for key in sorted(OFFICER_POST_POSITIVE_NUMBER_FIELDS):
        if key not in body:
            continue
        value = body[key]
        if value is None:
            officer_patch[key] = None
            continue
        if isinstance(value, bool):
            raise HTTPException(
                status_code=400, detail=f"{key} must be a positive USD number"
            )
        try:
            ceiling = float(value)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400, detail=f"{key} must be a positive USD number"
            ) from exc
        if not math.isfinite(ceiling) or ceiling <= 0:
            raise HTTPException(
                status_code=400, detail=f"{key} must be a positive USD number"
            )
        officer_patch[key] = ceiling

    for key in sorted(OFFICER_POST_INT_FIELDS):
        if key in body:
            if body[key] is None:
                officer_patch[key] = None  # revert to the platform default
                continue
            try:
                officer_patch[key] = max(0, int(body[key]))
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=400, detail=f"{key} must be an integer"
                ) from exc

    if "brain" in body:
        brain = body["brain"]
        if brain is None:
            # Clear the whole override — he thinks on the session default.
            llm_patch["model"] = None
            llm_patch["reasoning_level"] = None
            brain = {}
        if not isinstance(brain, dict) or (not brain and body["brain"] is not None):
            raise HTTPException(
                status_code=400,
                detail="brain must be an object of model and/or reasoning_level",
            )
        unknown_brain = set(brain) - {"model", "reasoning_level"}
        if unknown_brain:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown brain keys: {sorted(unknown_brain)}",
            )
        if "model" in brain:
            model = brain["model"]
            if model is None:
                llm_patch["model"] = None
            elif not isinstance(model, str) or not model.strip() or len(model) > 128:
                raise HTTPException(
                    status_code=400, detail="brain.model must be a short string"
                )
            else:
                llm_patch["model"] = model.strip()
        if "reasoning_level" in brain:
            # Same vocabulary gate as the create bridge — garbage fails loud,
            # the family capability still clamps at attach.
            llm_patch["reasoning_level"] = (
                None
                if brain["reasoning_level"] is None
                else validated_reasoning_level(brain["reasoning_level"])
            )

    if "communication_policy" in body:
        policy = body["communication_policy"]
        if policy is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "communication_policy cannot be cleared — send explicit "
                    "values for worker_messages and/or officer_response_minutes"
                ),
            )
        if not isinstance(policy, dict) or not policy:
            raise HTTPException(
                status_code=400,
                detail=(
                    "communication_policy must be an object of worker_messages "
                    "and/or officer_response_minutes"
                ),
            )
        unknown_policy = set(policy) - {
            "worker_messages",
            "officer_response_minutes",
        }
        if unknown_policy:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown communication_policy keys: {sorted(unknown_policy)}",
            )
        cleaned_policy: dict[str, Any] = {}
        if "worker_messages" in policy:
            if policy["worker_messages"] not in COMMUNICATION_WORKER_MESSAGES:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "worker_messages must be one of "
                        f"{sorted(COMMUNICATION_WORKER_MESSAGES)}"
                    ),
                )
            cleaned_policy["worker_messages"] = policy["worker_messages"]
        if "officer_response_minutes" in policy:
            lo, hi = COMMUNICATION_RESPONSE_MINUTES_BOUNDS
            try:
                minutes = int(policy["officer_response_minutes"])
            except (TypeError, ValueError) as exc:
                raise HTTPException(
                    status_code=400,
                    detail="officer_response_minutes must be an integer",
                ) from exc
            if not lo <= minutes <= hi:
                raise HTTPException(
                    status_code=400,
                    detail=f"officer_response_minutes must be between {lo} and {hi}",
                )
            cleaned_policy["officer_response_minutes"] = minutes
        comm_patch = cleaned_policy

    fragment: dict[str, Any] = {}
    if officer_patch:
        fragment["officer"] = officer_patch
    if llm_patch:
        fragment["llm"] = llm_patch
    effects = {key: OFFICER_POST_EFFECTS[key] for key in body}
    return fragment, comm_patch, effects


def enforce_officer_auto_pull_release(
    desired: Any, *, dependencies: OfficerPostPolicyDependencies
) -> None:
    """Refuse unattended enablement while the deployment release fence is dark.

    Callers pass the *effective* commissioned value, not merely the incoming
    patch. A post enabled before a rollback therefore cannot be recommissioned
    behind a dark fence. False/absent always passes so the supported stop path
    remains available during an incident.
    """

    # Historical JSON written before the typed surface may contain the exact
    # truthy shapes accepted by ``auto_pull_enabled``.  A rollback must fence
    # those too; otherwise a recommission could turn an unsupported string or
    # integer into live unattended authority behind the dark gate.
    if (
        desired in (True, "true", "True", 1)
        and not dependencies.auto_pull_release_enabled()
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "Officer auto-pull is not released in this deployment. "
                "Keep it off until the unattended-operation gates are complete."
            ),
        )


def check_officer_sleep_bounds(
    current_officer_cfg: dict[str, Any], officer_patch: dict[str, Any]
) -> None:
    """min ≤ max over the MERGED view (§7) — a patch of one bound is checked
    against the standing other bound, defaults 5/60 where nothing is set."""
    if not officer_patch or not (
        {"sleep_min_minutes", "sleep_max_minutes"} & set(officer_patch)
    ):
        return

    def _bound(key: str, default: int) -> int:
        if key in officer_patch:
            # A null patch value clears the bound — it reverts to the default.
            if officer_patch[key] is None:
                return default
            return int(officer_patch[key])
        try:
            return int(current_officer_cfg.get(key) or default)
        except (TypeError, ValueError):
            return default

    new_min = _bound("sleep_min_minutes", 5)
    new_max = _bound("sleep_max_minutes", 60)
    if new_min > new_max:
        raise HTTPException(
            status_code=400,
            detail=(
                f"sleep_min_minutes ({new_min}) must not exceed "
                f"sleep_max_minutes ({new_max})"
            ),
        )


OFFICER_NOTE_MAX_CHARS = 8000


def format_legate_note(user: dict[str, Any], message: str) -> str:
    """Stamp a note with its author before it reaches the officer.

    He treats a Legate directive as top authority, so who wrote it is part of
    the message: a note composed by an assistant holding the Legate's
    credentials must not read as words the Legate typed himself.
    """
    actor = str(user.get("display_name") or user.get("email") or "the Legate").strip()
    channel = "via MCP" if str(user.get("auth_method") or "") == "mcp" else "via API"
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"[Legate note — {actor} {channel}, {stamp}]\n\n{message}"


__all__ = [
    "COMMUNICATION_RESPONSE_MINUTES_BOUNDS",
    "COMMUNICATION_WORKER_MESSAGES",
    "OFFICER_CONFIG_NAME",
    "OFFICER_NOTE_MAX_CHARS",
    "OFFICER_PERMISSION_MODE",
    "OFFICER_POST_EFFECTS",
    "OFFICER_POST_INT_FIELDS",
    "OFFICER_POST_POSITIVE_NUMBER_FIELDS",
    "OfficerPostPolicyDependencies",
    "check_officer_sleep_bounds",
    "enforce_officer_auto_pull_release",
    "format_legate_note",
    "validated_officer_post_patch",
]
