"""Whether a job plans in phases, and the one config key that says so.

A job runs on the worker graph in one of two ways. Without phases (the
default) it gets a kickoff prompt, works with its tools and ends with
``job_complete``. With phases it follows the planned procedure: plan, work
through to-dos, review. That procedure is for long work that outlives several
compactions.

The key is an ordinary config key next to ``autonomy``: an expert sets it, a
project default or a job's override can change it, and it is frozen with the
rest of the job's configuration when the job is created. Everything the
setting changes is written into that frozen configuration at admission (see
``apply_no_phases`` in ``shared.runtime.core.loader``), so the worker does not
have to know the setting to run the job.

The value is a boolean. Expert manifests take JSON scalar spellings only, so
an expert file says ``phases: true``. Where a value arrives as text (a JSON
override written by hand), ``on`` and ``off`` are read as well.
"""

from __future__ import annotations

from typing import Any, Mapping

PHASES_KEY = "phases"

#: A job does not plan in phases unless its configuration says so.
DEFAULT_PHASES = False

# Tools that exist only for the phased procedure. A job without phases is not
# offered them: it has one task, and ``job_complete`` ends it.
# ``mark_complete`` looks like ``job_complete`` and ends nothing.
PHASE_PROCEDURE_TOOLS: tuple[str, ...] = (
    "next_phase_todos",
    "todo_complete",
    "todo_list",
    "request_replan",
    "mark_complete",
)

# Skills that carry the phased procedure. A job without phases gets the
# ``direct-task`` skill in their place.
PHASE_PROCEDURE_SKILLS: tuple[str, ...] = (
    "strategic-phase",
    "tactical-phase",
    "todo-guide",
    "verify-before-done",
)
DIRECT_TASK_SKILL = "direct-task"

_TRUE_WORDS = frozenset({"on", "true", "yes"})
_FALSE_WORDS = frozenset({"off", "false", "no"})


def _parse(value: Any) -> bool | None:
    """The boolean ``value`` states, or ``None`` when it states none."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        word = value.strip().lower()
        if word in _TRUE_WORDS:
            return True
        if word in _FALSE_WORDS:
            return False
    return None


def is_phases_value(value: Any) -> bool:
    """True when ``value`` states the setting: a boolean, or ``on`` / ``off``."""
    return _parse(value) is not None


def normalize_phases(value: Any) -> bool:
    """Return the setting ``value`` states; ``None`` and ``""`` mean the default.

    Raises ``ValueError`` for anything else, so a typo is refused where the
    value enters the system instead of silently running the default.
    """
    if value is None or value == "":
        return DEFAULT_PHASES
    parsed = _parse(value)
    if parsed is None:
        raise ValueError(f"phases must be true or false (on or off); got {value!r}")
    return parsed


def plans_in_phases(config: Mapping[str, Any] | None) -> bool:
    """Whether a config dict asks for phases; anything unreadable is the default.

    This is the lenient reader for stored and merged configuration. Values are
    validated where they enter (:func:`normalize_phases`).
    """
    if not isinstance(config, Mapping):
        return DEFAULT_PHASES
    parsed = _parse(config.get(PHASES_KEY))
    return DEFAULT_PHASES if parsed is None else parsed
