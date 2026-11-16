"""The two ways a job can run, and the one config key that names them.

``worker`` is the default: the planned procedure in phases (plan, work through
to-dos, review), for big tasks. ``agent`` does one small task in one loop and
ends it with ``job_complete``.

The key is an ordinary config key next to ``autonomy``: a job's override, a
project default or an expert can set it, and it is frozen with the rest of the
job's configuration when the job is created. Everything a mode changes is
written into that frozen configuration at admission (see
``apply_agent_job_mode`` in ``shared.runtime.core.loader``), so the worker does
not have to know the mode to run the job.
"""

from __future__ import annotations

from typing import Any, Literal, Mapping

JOB_MODE_KEY = "job_mode"

JOB_MODE_WORKER = "worker"
JOB_MODE_AGENT = "agent"
JOB_MODES: tuple[str, ...] = (JOB_MODE_WORKER, JOB_MODE_AGENT)
DEFAULT_JOB_MODE = JOB_MODE_WORKER

JobMode = Literal["worker", "agent"]

# Tools that exist only for the phased procedure. An agent-mode job is not
# offered them: it has one to-do, and ``job_complete`` closes it.
# ``mark_complete`` looks like ``job_complete`` and ends nothing.
PHASE_PROCEDURE_TOOLS: tuple[str, ...] = (
    "next_phase_todos",
    "todo_complete",
    "todo_list",
    "request_replan",
    "mark_complete",
)

# Skills that carry the phased procedure. An agent-mode job gets the
# ``agent-mode`` skill in their place.
PHASE_PROCEDURE_SKILLS: tuple[str, ...] = (
    "strategic-phase",
    "tactical-phase",
    "todo-guide",
    "verify-before-done",
)
AGENT_MODE_SKILL = "agent-mode"


def is_job_mode(value: Any) -> bool:
    """True when ``value`` is one of the two mode names."""
    return isinstance(value, str) and value in JOB_MODES


def normalize_job_mode(value: Any) -> str:
    """Return the mode ``value`` names; ``None`` and ``""`` mean the default.

    Raises ``ValueError`` for anything else, so a typo is refused where the
    value enters the system instead of silently running the default.
    """
    if value is None or value == "":
        return DEFAULT_JOB_MODE
    if is_job_mode(value):
        return value
    raise ValueError(f"job_mode must be one of {', '.join(JOB_MODES)}; got {value!r}")


def job_mode_of(config: Mapping[str, Any] | None) -> str:
    """The mode a config dict asks for; anything unreadable is the default.

    This is the lenient reader for stored and merged configuration. Values are
    validated where they enter (:func:`normalize_job_mode`).
    """
    if not isinstance(config, Mapping):
        return DEFAULT_JOB_MODE
    value = config.get(JOB_MODE_KEY)
    return value if is_job_mode(value) else DEFAULT_JOB_MODE


def is_agent_mode(config: Mapping[str, Any] | None) -> bool:
    """True when ``config`` asks for agent mode."""
    return job_mode_of(config) == JOB_MODE_AGENT
