"""Coding utility functions for the Universal Agent.

Provides shared utilities used by shell tools:
- _truncate_output: Truncate large output keeping the tail

The run_command tool has been removed — use the `shell` tool
from shell_tools.py instead, which runs commands in persistent
tmux-backed terminal tabs.
"""

import logging
from typing import Sequence

from shared.content_redaction import sanitize_tool_output
from shared.tool_catalog.definitions import (
    CODING_TOOLS_METADATA as CODING_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

# Default maximum output characters (stdout + stderr each)
DEFAULT_MAX_OUTPUT_CHARS = 50000

# Tool metadata for registry (empty — run_command removed)


# How far before the cut a credential can start and still reach past it. A
# token URL is a few hundred characters and a private key a few KB.
_CUT_MARGIN = 16_384


def _truncate_output(
    text: str,
    max_chars: int,
    label: str = "output",
    *,
    secrets: Sequence[str] = (),
) -> str:
    """Truncate output, keeping the tail (most useful for test output).

    Args:
        text: The output text to truncate
        max_chars: Maximum characters to keep
        label: Label for the truncation notice
        secrets: Known credential values (the workspace's own tokens)

    Returns:
        Truncated text with notice if truncation occurred
    """
    if len(text) <= max_chars:
        return text

    # Redact BEFORE cutting. The cut can land inside a credential-bearing
    # remote URL or a known token, and the surviving fragment — no scheme, no
    # `@`, not the whole token — is something no pattern downstream
    # recognizes any more. Only the kept tail plus a margin is scanned, so a
    # 5 MiB scrollback costs what a 66 KB one does. The window's own left edge
    # is a cut too: the first half of the margin is always discarded, which
    # is more than a fragment straddling that edge can occupy.
    window_start = len(text) - max_chars - _CUT_MARGIN
    window = text[max(window_start, 0) :]
    window = sanitize_tool_output(window, secrets=secrets).text
    if window_start <= 0 and len(window) <= max_chars:
        return window
    keep_from = len(window) - max_chars
    if window_start > 0:
        keep_from = max(keep_from, _CUT_MARGIN // 2)
    truncated = window[keep_from:]
    # Try to start at a line boundary
    first_newline = truncated.find("\n")
    if first_newline > 0 and first_newline < 200:
        truncated = truncated[first_newline + 1 :]

    chars_removed = len(text) - len(truncated)
    return f"[{label} truncated: {chars_removed} chars removed from start]\n{truncated}"


def create_coding_tools(context) -> list:
    """Create coding tools with injected context.

    Returns an empty list — run_command has been removed.
    The `shell` tool in shell_tools.py replaces it.
    Kept for backward compatibility with __init__.py imports.
    """
    return []
