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


def legacy_wire_permission_mode(mode: str) -> str:
    """The spelling an agent image from before the rename accepts on ``mode.set``.

    Only the thread-control inbox payload uses this, for as long as such images
    can run; the contract step of the rename removes it.
    """
    mode = normalize_permission_mode(mode)
    return "supervised" if mode == READ_ONLY else mode
