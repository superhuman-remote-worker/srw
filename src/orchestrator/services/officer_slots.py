"""Officer slot roster — typed worker allocations for centurion sessions.

The flat ``officer.max_concurrent_workers`` cap answers "how many workers
may he run"; slots answer "what KIND of workers does he command". The
Legate assigns a kit at provision time::

    officer:
      enabled: true
      slots:
        line:  {count: 2, model: "MiniMax-M3", backend: "sandbox"}
        heavy: {count: 1, model: "gpt-5.6-sol", backend: "vm"}

and the officer names a slot per dispatch (``create_job(...,
slot="heavy")``). The job-creation funnel stamps the slot's model/backend
onto the job config server-side — the officer chooses WHICH troops to send,
never what they are made of. An explicit per-job model/backend that disagrees
with the selected slot is rejected by the authoritative admission service
before job or ticket-claim insertion; compatible/defaulted requests receive
this patch. Per-slot count is enforced under the
durable Officer Post row lock (knowledge-base/knowledge/features/centurion.md §6).

Pure module by design: validation and admission are plain functions over
dicts so they unit-test without the FastAPI app. ``main.py`` owns the HTTP
translation (ValueError → 400 at provision, SlotAdmissionError → 409 at
job create). Slots are an ALLOCATION system, not a privilege one — the
capability-grant PDP (model_selection, can_use_vm) still gates the stamped
config downstream, so a slot can never exceed what its owner may grant.
"""

from __future__ import annotations

import math
import re
from typing import Any, Optional

from orchestrator.services.work_categories import WORK_CATEGORIES
from orchestrator.services.work_categories import (
    normalize_category as normalize_work_category,
)

# Workspace tiers a slot may pin. Mirrors the values the job funnel and
# dispatcher understand today; extend deliberately, not defensively.
ALLOWED_SLOT_BACKENDS = frozenset({"sandbox", "virtual", "none", "vm"})

_SLOT_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,23}$")
_MAX_SLOTS = 8
_MAX_SLOT_COUNT = 20
_SPEC_KEYS = frozenset({"count", "model", "backend", "category", "spend_ceiling_daily"})

# Jobs created before slots existed (or while the roster was flat) carry no
# stamp; they surface under this label in capacity reporting.
UNSLOTTED = "unslotted"


class SlotAdmissionError(Exception):
    """A dispatch does not fit the roster (funnel maps this to HTTP 409)."""


def validate_slots_spec(slots: Any) -> dict[str, dict[str, Any]]:
    """Validate + normalize an ``officer.slots`` override fragment.

    Returns the cleaned ``{name: {count, model?, backend?}}`` mapping.
    Raises ValueError with a legible message on any violation — the
    session-create sanitizer turns that into a 400 so a typo'd kit fails at
    provision time, not silently at the officer's first dispatch.
    """
    if not isinstance(slots, dict) or not slots:
        raise ValueError("officer.slots must be a non-empty object of named slots")
    if len(slots) > _MAX_SLOTS:
        raise ValueError(f"officer.slots supports at most {_MAX_SLOTS} slot types")
    cleaned: dict[str, dict[str, Any]] = {}
    for name, spec in slots.items():
        if not isinstance(name, str) or not _SLOT_NAME_RE.match(name):
            raise ValueError(
                f"slot name {name!r} invalid: lowercase alphanumeric plus "
                "dash/underscore, max 24 chars"
            )
        if name == UNSLOTTED:
            raise ValueError(f"slot name {UNSLOTTED!r} is reserved")
        if not isinstance(spec, dict):
            raise ValueError(f"slot {name!r} must be an object")
        unknown = set(spec) - _SPEC_KEYS
        if unknown:
            raise ValueError(f"slot {name!r} has unknown keys: {sorted(unknown)}")
        try:
            count = int(spec["count"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"slot {name!r} needs an integer count") from exc
        if not 0 <= count <= _MAX_SLOT_COUNT:
            raise ValueError(
                f"slot {name!r} count must be between 0 and {_MAX_SLOT_COUNT}"
            )
        entry: dict[str, Any] = {"count": count}
        model = spec.get("model")
        if model is not None:
            if not isinstance(model, str) or not model.strip() or len(model) > 128:
                raise ValueError(f"slot {name!r} model must be a short string")
            entry["model"] = model.strip()
        backend = spec.get("backend")
        if backend is not None:
            if backend not in ALLOWED_SLOT_BACKENDS:
                raise ValueError(
                    f"slot {name!r} backend must be one of "
                    f"{sorted(ALLOWED_SLOT_BACKENDS)}"
                )
            entry["backend"] = backend
        category = spec.get("category")
        if category is not None:
            # A slot with a category is a POOL: the auto-pull tick may fill it
            # from ready tickets of that category. A slot without one keeps
            # today's behaviour exactly — officer-directed dispatch only, never
            # touched by the tick (officer_backlog_pools.md §6).
            if normalize_work_category(category) is None:
                raise ValueError(
                    f"slot {name!r} category must be one of {sorted(WORK_CATEGORIES)}"
                )
            entry["category"] = str(category).strip().lower()
        ceiling = spec.get("spend_ceiling_daily")
        if ceiling is not None:
            # Optional and unset by default (§13.3). A MiniMax research pool
            # and a frontier executor pool differ by more than an order of
            # magnitude in burn, so there is no global number worth defaulting
            # to — the knob belongs beside the model and backend it prices.
            if isinstance(ceiling, bool):
                raise ValueError(
                    f"slot {name!r} spend_ceiling_daily must be a number of USD"
                )
            try:
                ceiling_value = float(ceiling)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"slot {name!r} spend_ceiling_daily must be a number of USD"
                ) from exc
            if not math.isfinite(ceiling_value) or ceiling_value <= 0:
                raise ValueError(
                    f"slot {name!r} spend_ceiling_daily must be positive "
                    "(omit the key for no ceiling)"
                )
            entry["spend_ceiling_daily"] = ceiling_value
        cleaned[name] = entry
    return cleaned


def roster_from_meta(officer_meta: dict[str, Any]) -> Optional[dict[str, dict]]:
    """The slot roster from a thread's officer metadata, or None (flat cap).

    Deliberately forgiving on read: the spec was validated at provision, so
    anything malformed here (hand-edited metadata) degrades to the flat cap
    rather than wedging every dispatch.
    """
    slots = officer_meta.get("slots")
    if not isinstance(slots, dict) or not slots:
        return None
    cleaned: dict[str, dict] = {}
    for name, spec in slots.items():
        if not isinstance(name, str) or not isinstance(spec, dict):
            return None
        try:
            count = int(spec.get("count"))
        except (TypeError, ValueError):
            return None
        cleaned[name] = {**spec, "count": count}
    return cleaned


def admit(
    officer_meta: dict[str, Any],
    requested_slot: Optional[str],
    in_flight_by_slot: dict[Optional[str], int],
) -> tuple[Optional[str], dict[str, Any]]:
    """Decide whether one more dispatch fits the officer's allocation.

    Returns ``(slot_name, config_patch)`` — slot_name is None on the flat-cap
    path; config_patch is the slot's authoritative model/backend fragment.
    The caller must reject any contradictory explicit per-job choice before
    merging this patch; :mod:`services.officer_admission` does so both during
    preparation and again under the Post lock. Raises SlotAdmissionError with
    an actionable message when the dispatch does not fit.

    ``in_flight_by_slot`` maps stamp → count for the officer's slot-occupying
    jobs — non-terminal minus paused, which vacates its slot (None key =
    pre-roster jobs); the caller reads it under the same durable post lock
    that serializes parallel creates across incarnations.
    """
    roster = roster_from_meta(officer_meta)

    if roster is None:
        # Flat path — unchanged semantics from S5. Sum everything he has in
        # flight regardless of stamps.
        try:
            cap = max(1, int(officer_meta.get("max_concurrent_workers") or 3))
        except (TypeError, ValueError):
            cap = 3
        total = sum(in_flight_by_slot.values())
        if total >= cap:
            raise SlotAdmissionError(
                f"Officer capacity reached: {total}/{cap} worker jobs already "
                "in flight. Wait for one to finish, cancel one, or raise "
                "max_concurrent_workers."
            )
        return None, {}

    def _free(name: str) -> int:
        return int(roster[name]["count"]) - in_flight_by_slot.get(name, 0)

    slot_lines = ", ".join(
        f"{name} ({_free(name)}/{roster[name]['count']} free)"
        for name in sorted(roster)
    )

    name = requested_slot
    if name is None:
        if len(roster) == 1:
            name = next(iter(roster))
        else:
            raise SlotAdmissionError(
                "This officer commands typed worker slots — name one via the "
                f"slot argument. Your roster: {slot_lines}."
            )
    if name not in roster:
        raise SlotAdmissionError(f"Unknown slot '{name}'. Your roster: {slot_lines}.")
    if _free(name) <= 0:
        raise SlotAdmissionError(
            f"Slot '{name}' is full: {in_flight_by_slot.get(name, 0)}/"
            f"{roster[name]['count']} in flight. Wait for one to finish, "
            f"cancel one, or use another slot ({slot_lines})."
        )

    patch: dict[str, Any] = {}
    if roster[name].get("model"):
        patch["llm"] = {"model": roster[name]["model"]}
    if roster[name].get("backend"):
        patch["workspace"] = {"backend": roster[name]["backend"]}
    return name, patch


def below_floor_pools(
    officer_meta: dict[str, Any],
    ready_by_pool: Optional[dict[str, int]],
) -> list[str]:
    """Categorized pools whose ready queue sits under its floor.

    The floor IS the pool's slot count (§13.2). This is the single source of
    truth for that predicate: :func:`capacity_lines` marks the starved pools
    and the wake's closing call-to-action names them, and the two must never
    disagree about which pools are short — a sitrep that marks a pool BELOW
    FLOOR while the closing line says nothing reads as a number to note
    rather than a thing to do.
    """
    if ready_by_pool is None:
        return []
    # The floor is auto-pull's queue. With auto-pull OFF an armed ticket is
    # never dispatched, so a starved-pool marker only tells the officer to
    # manufacture tickets nobody will pull: on k3d (2026-11-05) a new officer's
    # first wake invented a "CI build" ticket for a project with no CI. The
    # floor wake is already dormant in that state; the sitrep now agrees. Same
    # predicate as ``officer_backlog.auto_pull_enabled`` (not imported: that
    # module imports this one).
    if officer_meta.get("auto_pull") not in (True, "true", "True", 1):
        return []
    roster = roster_from_meta(officer_meta)
    if roster is None:
        return []
    return [
        name
        for name in sorted(roster)
        if roster[name].get("category")
        and ready_by_pool.get(name, 0) < roster[name]["count"]
    ]


def capacity_lines(
    officer_meta: dict[str, Any],
    in_flight_by_slot: dict[Optional[str], int],
    *,
    ready_by_pool: Optional[dict[str, int]] = None,
    oldest_claim_age_hours: Optional[float] = None,
) -> str:
    """One sitrep line describing capacity, slot-aware.

    "Capacity: heavy 0/1, line 1/2 worker slots in use." for a roster;
    the flat "N/cap worker slots in use." otherwise.

    For categorized pools the line carries what the officer actually steers by
    (officer_backlog_pools.md §6). Utilization alone answers "am I busy",
    which is the wrong question — an idle slot with a healthy queue is slack,
    and slack is fine. The question that matters is whether the QUEUE is
    starved, so each pool renders ``in-use/count ready:N`` and a pool below its
    floor is marked. ``oldest_claim_age_hours`` is the counterweight: a deep
    queue with a three-day-old claim is not a healthy pool, it is a stuck one.

    ``ready_by_pool`` counts tickets the tick would actually dispatch —
    ready, categorized, unambiguous and unclaimed — not everything wearing a
    ``ready`` tag. A number the officer cannot act on is worse than no number.
    """
    roster = roster_from_meta(officer_meta)
    if roster is None:
        try:
            cap = max(1, int(officer_meta.get("max_concurrent_workers") or 3))
        except (TypeError, ValueError):
            cap = 3
        total = sum(in_flight_by_slot.values())
        return f"Capacity: {total}/{cap} worker slots in use."

    # The floor IS the pool's slot count (§13.2): if every agent in the pool
    # lands at once, each must find a ticket waiting. Shared with the wake's
    # closing call-to-action through below_floor_pools() so the marker and the
    # instruction cannot drift apart.
    starved = set(below_floor_pools(officer_meta, ready_by_pool))

    parts = []
    for name in sorted(roster):
        count = roster[name]["count"]
        segment = f"{name} {in_flight_by_slot.get(name, 0)}/{count}"
        if ready_by_pool is not None and roster[name].get("category"):
            ready = ready_by_pool.get(name, 0)
            # Parenthesised so the sentence still parses — an unbracketed
            # "BELOW FLOOR" ran straight into the trailing "worker slots in
            # use" and read as if it described the slots rather than the queue.
            flag = ", BELOW FLOOR" if name in starved else ""
            segment += f" (ready {ready}{flag})"
        parts.append(segment)

    stray = in_flight_by_slot.get(None, 0)
    tail = f" (+{stray} unslotted)" if stray else ""
    line = f"Capacity: {', '.join(parts)} worker slots in use{tail}."
    if oldest_claim_age_hours is not None:
        line += f" Oldest open claim {oldest_claim_age_hours:.0f}h."
    return line
