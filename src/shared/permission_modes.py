"""Session permission modes, and the one place their legacy spelling is mapped.

``read_only`` was called ``supervised`` until 2026-10
(knowledge-base/knowledge/features/tool_permission_tiers.md, D2). Stored rows,
immutable journals, exported manifests and agent images from before the rename
still carry the old name, so every reader normalises through
:func:`normalize_permission_mode` instead of comparing raw strings.
"""

from typing import Any

READ_ONLY = "read_only"
AUTO_ACCEPT = "auto_accept"
AUTONOMOUS = "autonomous"

# Most to least restrictive. Grant ceilings compare positions in this tuple.
PERMISSION_MODES = (READ_ONLY, AUTO_ACCEPT, AUTONOMOUS)
DEFAULT_PERMISSION_MODE = READ_ONLY

# Old spelling -> canonical name.
LEGACY_PERMISSION_MODES = {"supervised": READ_ONLY}

# Every spelling a reader must accept: the canonical names plus the legacy ones.
ACCEPTED_PERMISSION_MODES = frozenset(PERMISSION_MODES) | frozenset(
    LEGACY_PERMISSION_MODES
)


def normalize_permission_mode(value: Any) -> Any:
    """Return the canonical name for ``value``; anything unknown is returned as is.

    Unknown values pass through unchanged so validators still reject them.
    """
    if isinstance(value, str):
        return LEGACY_PERMISSION_MODES.get(value, value)
    return value


def is_permission_mode(value: Any) -> bool:
    """True for a canonical name or a legacy spelling of one."""
    return normalize_permission_mode(value) in PERMISSION_MODES


# What a tool call does, declared per tool next to its definition (the tool
# catalog's ``effect`` key). The mode decides which effects ask first.
READ = "read"  # observes only; may save what it fetched into the workspace
BOOKKEEPING = "bookkeeping"  # the session's own tasks, canvas, pauses
WRITE = "write"  # changes workspace files or project state
EXTERNAL = "external"  # irreversible, or acts outside the workspace
SHELL = "shell"  # runs a command

TOOL_EFFECTS = (READ, BOOKKEEPING, WRITE, EXTERNAL, SHELL)

# A tool that declares nothing (an MCP tool without hints, a tool someone
# forgot to tag) asks in every mode below autonomous.
UNDECLARED_EFFECT = EXTERNAL

# Effects each mode runs without asking. An unknown mode runs nothing freely.
_RUNS_FREELY = {
    READ_ONLY: frozenset({READ, BOOKKEEPING}),
    AUTO_ACCEPT: frozenset({READ, BOOKKEEPING, WRITE}),
    AUTONOMOUS: frozenset(TOOL_EFFECTS),
}


def effect_asks(mode: Any, effect: Any) -> bool:
    """Whether a call with ``effect`` waits for approval under ``mode``.

    Fails closed: an unknown mode asks for everything, and an unknown effect
    is treated as :data:`UNDECLARED_EFFECT`.
    """
    if effect not in TOOL_EFFECTS:
        effect = UNDECLARED_EFFECT
    return effect not in _RUNS_FREELY.get(normalize_permission_mode(mode), frozenset())


def legacy_wire_permission_mode(mode: str) -> str:
    """The spelling an agent image from before the rename accepts on ``mode.set``.

    Only the thread-control inbox payload uses this, for as long as such images
    can run; the contract step of the rename removes it.
    """
    mode = normalize_permission_mode(mode)
    return "supervised" if mode == READ_ONLY else mode
