"""Shell completion and timeout protocol shared by workspace transports.

These constants and pure helpers contain no tool registration or execution
policy. ShellManager and RemoteBackend use the same sentinel/polling contract.
"""

import re
import uuid
from typing import List, Optional, Tuple


def new_start_marker() -> str:
    """Return a unique line that marks where one command's output starts."""
    return f"__SRW_START_{uuid.uuid4().hex[:12]}__"


def start_marker_command(start_marker: str) -> str:
    """Return the command that prints ``start_marker`` on a line of its own.

    It assembles the marker at run time, so the echo of the typed command
    contains this command text but never the marker itself: only the printed
    line matches the marker exactly.
    """
    token = start_marker.removeprefix("__SRW_START_").removesuffix("__")
    return f"printf '__SRW_START_%s__\\n' {token}"


def build_sentinel_command(
    command: str,
    sentinel: str,
    start_marker: Optional[str] = None,
) -> str:
    """Build the command string to send to tmux for sentinel-based completion.

    With ``start_marker`` the command prints that line before it runs, so a
    caller that extracts the output takes the lines between the marker and
    the sentinel and never has to tell output from the typed command's echo.

    Single-line commands use simple ';' chaining: this preserves the
    existing interactive-prompt detection semantics — when a single-line
    command waits on stdin (e.g. `read`, ssh password prompt), the polling
    loop sees the prompt and reports it.

    Multi-line commands are wrapped in an outer bash heredoc so the user's
    command (including any inner heredocs like `python3 <<'PY' ... PY`) is
    read by inner bash from a captured heredoc body, instead of being typed
    line-by-line into tmux. This fixes BUG-5: previously the heredoc
    terminator landed on the same line as the sentinel echo (`PY; echo ...`)
    and the heredoc never closed, leaving the tab permanently stuck.
    """
    mark = start_marker_command(start_marker) if start_marker else None
    if "\n" not in command:
        prefix = f"{mark}; " if mark else ""
        return f'{prefix}{command}; printf \'\\n{sentinel} %s %s\\n\' "$?" "$PWD"'

    outer_delim = f"SRW_DELIM_{uuid.uuid4().hex[:12]}"
    mark_line = f"{mark}\n" if mark else ""
    return (
        f'bash << "{outer_delim}"\n'
        f"{mark_line}"
        f"{command}\n"
        f'printf "\\n{sentinel} %s %s\\n" "$?" "$PWD"\n'
        f"{outer_delim}"
    )


# Auto-detected tab types based on command prefix
COMMAND_TYPE_MAP = {
    "ssh": "ssh",
    "python": "repl",
    "python3": "repl",
    "ipython": "repl",
    "jupyter": "repl",
    "node": "repl",
    "psql": "repl",
    "mysql": "repl",
    "mongosh": "repl",
    "redis-cli": "repl",
}

# Sentinel returned by _check_blocked when sudo is intercepted (freeze mode)
SUDO_FREEZE_SENTINEL = "SUDO_FREEZE_REQUESTED"

# Default blocked commands (sudo handled separately via sudo_action)
DEFAULT_BLOCKED_COMMANDS = frozenset(
    [
        "reboot",
        "shutdown",
        "poweroff",
        "halt",
        "init",
    ]
)

# Patterns that indicate the terminal is waiting for interactive input.
# Each entry is a tuple of (compiled_regex, description).
INTERACTIVE_PROMPT_PATTERNS = [
    # Yes/No confirmation prompts
    (
        re.compile(
            r"\[y/n\]|\[Y/n\]|\[y/N\]|\[N/y\]|\(yes/no\)|\(yes/no/\[fingerprint\]\)",
            re.IGNORECASE,
        ),
        "confirmation prompt",
    ),
    # Password / passphrase prompts
    (re.compile(r"(?:password|passphrase)\s*:", re.IGNORECASE), "password prompt"),
    # SSH host key verification
    (
        re.compile(r"Are you sure you want to continue connecting", re.IGNORECASE),
        "SSH host key verification",
    ),
    # PackageKit / dnf install prompts (Fedora)
    (
        re.compile(r"Install package '.*?' to provide command", re.IGNORECASE),
        "package install prompt",
    ),
    # sudo password
    (re.compile(r"\[sudo\] password for", re.IGNORECASE), "sudo password prompt"),
    # Press any key / press enter
    (
        re.compile(r"press any key|press enter to continue|hit enter", re.IGNORECASE),
        "press key prompt",
    ),
    # GPG passphrase
    (re.compile(r"enter passphrase", re.IGNORECASE), "passphrase prompt"),
]

# Seconds of *no new output* before a still-running command yields control back
# to the model (the "soft" no-change timeout). Generous on purpose: heavy
# installs/builds (e.g. pip downloading torch + CUDA wheels) routinely go quiet
# for many seconds without waiting for input. Matches OpenHands' default.
NO_CHANGE_TIMEOUT_SECONDS = 30.0

# Absolute ceiling on how long a single synchronous command may block, even
# when the caller passes a larger timeout.
HARD_TIMEOUT_CAP_SECONDS = 600

# Non-interactive environment applied to every fresh shell, so that pagers,
# progress bars and credential prompts can't stall the no-change detector or
# hang the command. (Deliberately does NOT set TERM=dumb — too disruptive.)
# Agent shells also keep no history file: bash writes ~/.bash_history when the
# tab is killed, and a typed command (a token clone URL) would otherwise land
# in the home that snapshots and the PVC keep. Unsetting HISTFILE after
# .bashrc has run covers the whole life of the shell.
NONINTERACTIVE_ENV_EXPORT = (
    "export PAGER=cat GIT_PAGER=cat GIT_TERMINAL_PROMPT=0 GIT_ASKPASS= "
    "SSH_ASKPASS= DEBIAN_FRONTEND=noninteractive PIP_PROGRESS_BAR=off "
    "PIP_DISABLE_PIP_VERSION_CHECK=1; unset HISTFILE"
)

# Returned when a command has produced no new output for NO_CHANGE_TIMEOUT_SECONDS
# (the soft no-change timeout). Leads with "Exit code: -1" so downstream parsers
# read it as "not finished" (distinct from success 0 and generic failure 1).
# This is NOT an error — the process keeps running on its tab.
#
# Guidance here is mode-NEUTRAL: it must be valid for every shell tool set. The
# stateless set (run_command + shell_read) cannot send keys or use other tabs,
# but it DOES have a dedicated cancel_command tool to abort a wedged tab — the
# run_command tool layer appends that pointer to these results (shell_tools.py),
# keeping this backend text tool-agnostic. Persistent-mode options (C-c via
# shell_execute keys mode, extra tabs) are taught by that tool's own docstring.
STILL_RUNNING_TEMPLATE = (
    "Exit code: -1\n"
    "--- still running ---\n"
    "Command on tab '{tab}' has been running {elapsed:.0f}s with no new output "
    "in the last {quiet:.0f}s — it has NOT finished (this is NOT an error; the "
    "process is still executing on the tab).\n"
    "Read the tab again after a moment to check for new output or completion; "
    "the tab stays busy until it finishes, so don't send it another command yet "
    "and don't poll in a tight loop. Tip: for work you expect to be slow or "
    "quiet (large installs, builds, data ingestion/embedding, downloads), pass "
    "an explicit `timeout` when you START the command so the call waits for the "
    "full duration instead of returning here.\n"
    "--- terminal state ---\n{terminal_state}"
)

# Returned when a command hits the hard timeout cap (the maximum a single call
# will wait) without completing. Distinct from the soft message above: the
# process may have been emitting output the whole time, so this must NOT claim
# the output went quiet.
STILL_RUNNING_HARDCAP_TEMPLATE = (
    "Exit code: -1\n"
    "--- still running ---\n"
    "Command on tab '{tab}' is still running after {elapsed:.0f}s — that is the "
    "maximum wait for one call, not an error, and it may still be producing "
    "output.\n"
    "Read the tab again to keep monitoring; the tab stays busy until it "
    "finishes. If you expect it to take much longer, start such commands with a "
    "larger explicit `timeout`.\n"
    "--- terminal state ---\n{terminal_state}"
)

# Returned when a new command is sent to a tab whose previous command is still
# running. The new command is NOT executed (avoids head-of-line blocking the
# tab with two interleaved commands). Mode-NEUTRAL guidance only (see above).
COLLIDING_COMMAND_TEMPLATE = (
    "Tab '{tab}' has a previous command still running; your new command was "
    "NOT executed.\n"
    "Wait for it to finish before sending another command here — read the tab "
    "to monitor its progress.\n"
    "--- terminal state ---\n{terminal_state}"
)

# Returned when the command is genuinely blocked on an interactive prompt
# (password, y/n, etc.). Unlike a no-change stall, this one really does need
# input — the model should respond in keys mode.
INTERACTIVE_PROMPT_TEMPLATE = (
    "Interactive prompt detected ({prompt_type}). The command is waiting for "
    "input on tab '{tab}'.\n"
    "Respond by sending the expected input (or a control key like C-c to "
    "cancel) in keys mode.\n"
    "--- terminal state ---\n{terminal_state}"
)


def compute_no_change_state(
    all_lines: List[str],
    prev_hash: Optional[int],
    stall_start: Optional[float],
    now: float,
    soft_enabled: bool,
    threshold: float,
) -> Tuple[int, Optional[float], bool]:
    """Track whether a running command's output has gone quiet long enough.

    Hashes the FULL captured buffer (not just the visible tail) so output
    scrolling anywhere — e.g. a long ``pip`` download printing above the
    visible lines — counts as activity and resets the clock. The previous
    implementation hashed only ``all_lines[-20:]`` and so mistook steady
    long-running work for a stall.

    Args:
        all_lines: Current full pane capture, as lines.
        prev_hash: Hash returned by the previous call (None on the first poll).
        stall_start: Monotonic time the current no-change streak began, or None.
        now: Current monotonic time.
        soft_enabled: Whether the soft no-change timeout applies. When the
            caller supplied an explicit timeout this is False, so only the
            caller's hard timeout bounds the command.
        threshold: Seconds of no change before declaring a soft timeout.

    Returns:
        ``(new_hash, new_stall_start, timed_out)``. ``timed_out`` is True only
        when ``soft_enabled`` and output has been unchanged for >= ``threshold``.
    """
    new_hash = hash(tuple(all_lines))
    if new_hash != prev_hash:
        # Output changed (or first observation) -> reset the no-change clock.
        return new_hash, None, False
    # Output unchanged since the previous poll.
    if stall_start is None:
        stall_start = now
    timed_out = soft_enabled and (now - stall_start >= threshold)
    return new_hash, stall_start, timed_out


def prompt_is_ready(all_lines: List[str]) -> bool:
    """True when the shell appears idle at a prompt.

    The last non-blank line ending in ``$``, ``#`` or ``%`` means bash is back
    at its prompt — i.e. a previously-running command has finished or been
    interrupted, even when its completion sentinel never printed (e.g. after a
    C-c). Used by the colliding-command guard to know when a tab is free again.
    """
    for line in reversed(all_lines):
        stripped = line.strip()
        if stripped:
            return stripped[-1] in ("$", "#", "%")
    return False
