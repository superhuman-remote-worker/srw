"""Session delegation settings of the ``delegation`` block (parallel_subagents.md §6.4).

A session parent has its own concurrency cap, because ``delegation.max_concurrent``
is the worker cap and an expert's worker-oriented value overrides the role
overlay (F14). Every session parent runs its children under it. The session
cap resolves in this order, and the result is clamped to
``SESSION_MAX_CONCURRENT_MIN..SESSION_MAX_CONCURRENT_MAX``:

1. the code default, ``SESSION_MAX_CONCURRENT_DEFAULT``;
2. the parent's model-family value — ``settings.session_max_concurrent`` in
   ``config/model_config_matrix.yaml``, which the settings matrix routes to
   ``delegation.family_session_max_concurrent`` (keyed on the PARENT's family,
   because the parent issues the calls; re-derived on every matrix pass, so a
   model switch replaces it);
3. an explicit expert or session value, ``delegation.session_max_concurrent``.

The cap limits concurrency, not the number of calls: calls above it queue and
run in waves. ``delegation.session_max_calls_per_turn`` bounds the calls of
one parent turn across all its batches (enforced by the live loop, WP3b).

Whether a session may fan out at all is not configuration: the orchestrator
advertises its batch-settle capability and its operator switch per claim or
attach (§12, D5), and ``agent.tools.delegation.fanout`` combines them. No key
of this block opens fan-out, so a rollback never waits for a session's frozen
config.

Pure functions over the plain ``delegation`` mapping, so the loader's parser,
the tool description and the runtime semaphore read one resolution.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

#: The explicit expert/session value.
SESSION_MAX_CONCURRENT_KEY = "session_max_concurrent"
#: The parent family's value, written by the settings matrix (never authored:
#: every matrix pass overwrites or removes it).
FAMILY_SESSION_MAX_CONCURRENT_KEY = "family_session_max_concurrent"
#: The flat ``settings`` key a family block of the matrix may carry.
MATRIX_SESSION_MAX_CONCURRENT_KEY = "session_max_concurrent"

SESSION_MAX_CONCURRENT_DEFAULT = 6
SESSION_MAX_CONCURRENT_MIN = 1
SESSION_MAX_CONCURRENT_MAX = 20

SESSION_MAX_CALLS_PER_TURN_KEY = "session_max_calls_per_turn"
SESSION_MAX_CALLS_PER_TURN_DEFAULT = 20
SESSION_MAX_CALLS_PER_TURN_MIN = 1
# One settle request carries at most 64 members
# (``shared.session_subagent_batch.BATCH_MAX_MEMBERS``); a turn is never allowed
# more calls than one settle can report.
SESSION_MAX_CALLS_PER_TURN_MAX = 64


def _as_int(raw: Any) -> Optional[int]:
    """``raw`` as an int, or None when absent or not an integer value."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError, OverflowError):  # OverflowError: YAML .inf
        return None


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def clamp_session_max_concurrent(raw: Any) -> Optional[int]:
    """A session cap value clamped to its range; None when unusable."""
    value = _as_int(raw)
    if value is None:
        return None
    return _clamp(value, SESSION_MAX_CONCURRENT_MIN, SESSION_MAX_CONCURRENT_MAX)


def clamp_session_max_calls_per_turn(raw: Any) -> Optional[int]:
    """A per-turn maximum clamped to its range; None when unusable."""
    value = _as_int(raw)
    if value is None:
        return None
    return _clamp(value, SESSION_MAX_CALLS_PER_TURN_MIN, SESSION_MAX_CALLS_PER_TURN_MAX)


def _mapping(delegation: Any) -> Mapping[str, Any]:
    return delegation if isinstance(delegation, Mapping) else {}


def session_max_concurrent(delegation: Any) -> int:
    """How many children a session parent runs at once.

    The explicit value, else the parent family's value, else the code default;
    always inside the allowed range.
    """
    block = _mapping(delegation)
    for key in (SESSION_MAX_CONCURRENT_KEY, FAMILY_SESSION_MAX_CONCURRENT_KEY):
        value = clamp_session_max_concurrent(block.get(key))
        if value is not None:
            return value
    return SESSION_MAX_CONCURRENT_DEFAULT


def session_max_calls_per_turn(delegation: Any) -> int:
    """How many delegate calls one session parent turn may make in total."""
    value = clamp_session_max_calls_per_turn(
        _mapping(delegation).get(SESSION_MAX_CALLS_PER_TURN_KEY)
    )
    return SESSION_MAX_CALLS_PER_TURN_DEFAULT if value is None else value


__all__ = [
    "FAMILY_SESSION_MAX_CONCURRENT_KEY",
    "MATRIX_SESSION_MAX_CONCURRENT_KEY",
    "SESSION_MAX_CALLS_PER_TURN_DEFAULT",
    "SESSION_MAX_CALLS_PER_TURN_KEY",
    "SESSION_MAX_CALLS_PER_TURN_MAX",
    "SESSION_MAX_CALLS_PER_TURN_MIN",
    "SESSION_MAX_CONCURRENT_DEFAULT",
    "SESSION_MAX_CONCURRENT_KEY",
    "SESSION_MAX_CONCURRENT_MAX",
    "SESSION_MAX_CONCURRENT_MIN",
    "clamp_session_max_calls_per_turn",
    "clamp_session_max_concurrent",
    "session_max_calls_per_turn",
    "session_max_concurrent",
]
