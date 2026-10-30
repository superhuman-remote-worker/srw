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


# A tab's shell outlives the commands typed into it, so a builtin that ends
# the shell ends the tab, not the command. Codex and Claude Code run every
# command in a fresh ``bash -c``, where ``...; exit "$rc"`` merely sets the exit
# code, and models write it that way (dev jobs 19837f4b and 1dc0fac1 lost their
# shell to ``exit "$result"``; OpenHands met the same, software-agent-sdk#3037).
_SHELL_ENDING_BUILTINS = frozenset({"exit", "logout", "exec"})

# Words after which the next word is still in command position.
_COMMAND_PREFIX_WORDS = frozenset(
    ("!", "{", "builtin", "command", "do", "elif", "else", "if", "then", "time")
    + ("until", "while")
)

_ASSIGNMENT_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\[[^\]]*\])?\+?=")

# ``die() {``: the body of a function the same command line may call
# (``... || die "msg"``) is read as if it ran here.
_FUNCTION_HEAD = re.compile(r"[^\s()]+\(\)")

# Characters that end an unquoted word.
_WORD_BREAKS = frozenset(" \t\n;&|()<>")

# What ends a tab's shell, as the model-facing messages name it.
SHELL_ENDING_CAUSES = (
    "A top-level `exit`, `logout` or `exec`, or a failing command after `set -e`"
)

# The shell of one tab exited; the other tabs live on. ``_lost_tab_message``.
TAB_SHELL_ENDED_TEMPLATE = (
    "Tab '{tab}' no longer exists: its shell exited. "
    + SHELL_ENDING_CAUSES
    + " ends a tab's shell. Use '{tab}' again to open a fresh shell there "
    "(working directory and variables start over). Open tabs: {tabs}."
)

# The shell a synchronous command ran in exited before the command finished.
COMMAND_SHELL_ENDED_TEMPLATE = (
    "The shell in tab '{tab}' exited while your command ran, so its output and "
    "exit code are lost. "
    + SHELL_ENDING_CAUSES
    + " ends the tab's shell; leave those out, the exit code is reported "
    "without them. {reset}"
)
TAB_RESET_NOTE = (
    "The next command on '{tab}' opens a fresh shell there (working directory "
    "and variables start over)."
)
SESSION_RESET_NOTE = (
    "The whole shell session ended with it, so SRW started a new one: every "
    "tab starts over (working directory, variables and processes started in "
    "them are gone)."
)

# The shell session was found ended at the start of a call, before it did
# anything; SRW started a new one.
SESSION_ENDED_MESSAGE = (
    "The shell session had ended (the shell of its last open tab exited) and "
    "SRW started a new one: every tab starts over (working directory, "
    "variables and processes started in them are gone). "
    + SHELL_ENDING_CAUSES
    + " ends a tab's shell. Nothing ran in this call; repeat it if you still "
    "need it."
)


def command_shell_ended_message(tab: str, *, session_reset: bool) -> str:
    """Tell the model that the shell its command ran in exited mid-command."""
    reset = SESSION_RESET_NOTE if session_reset else TAB_RESET_NOTE.format(tab=tab)
    return COMMAND_SHELL_ENDED_TEMPLATE.format(tab=tab, reset=reset)


def _skip_single_quoted(text: str, i: int) -> int:
    """Index after the ``'`` that closes the quote opened before ``i``."""
    end = text.find("'", i)
    return len(text) if end < 0 else end + 1


def _skip_ansi_c_quoted(text: str, i: int) -> int:
    """Index after the ``'`` that closes a ``$'...'`` quote opened before ``i``."""
    while i < len(text):
        if text[i] == "\\":
            i += 2
        elif text[i] == "'":
            return i + 1
        else:
            i += 1
    return len(text)


def _skip_backquoted(text: str, i: int) -> int:
    """Index after the backquote that closes the substitution opened before ``i``."""
    while i < len(text):
        if text[i] == "\\":
            i += 2
        elif text[i] == "`":
            return i + 1
        else:
            i += 1
    return len(text)


def _skip_double_quoted(text: str, i: int) -> int:
    """Index after the ``"`` that closes the quote opened before ``i``."""
    while i < len(text):
        char = text[i]
        if char == "\\":
            i += 2
        elif char == '"':
            return i + 1
        elif text.startswith("$(", i):
            i = _skip_nested(text, i + 2, "(", ")")
        elif char == "`":
            i = _skip_backquoted(text, i + 1)
        else:
            i += 1
    return len(text)


def _skip_nested(text: str, i: int, opening: str, closing: str) -> int:
    """Index after the ``closing`` that balances an ``opening`` before ``i``."""
    depth = 1
    while i < len(text):
        char = text[i]
        if char == "\\":
            i += 2
            continue
        if char == "'":
            i = _skip_single_quoted(text, i + 1)
            continue
        if char == '"':
            i = _skip_double_quoted(text, i + 1)
            continue
        if char == "`":
            i = _skip_backquoted(text, i + 1)
            continue
        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(text)


def _read_heredoc_delimiter(text: str, i: int) -> Tuple[str, int]:
    """The delimiter word at ``i`` with its quotes removed, and where it ends."""
    word: List[str] = []
    while i < len(text) and text[i] not in _WORD_BREAKS:
        char = text[i]
        if char in "'\"":
            end = text.find(char, i + 1)
            end = len(text) if end < 0 else end
            word.append(text[i + 1 : end])
            i = end + 1
        elif char == "\\":
            word.append(text[i + 1 : i + 2])
            i += 2
        else:
            word.append(char)
            i += 1
    return "".join(word), i


def _skip_heredoc_bodies(text: str, i: int, heredocs: List[Tuple[str, bool]]) -> int:
    """Index after the bodies of ``heredocs``, which start at line ``i``."""
    for delimiter, strip_tabs in heredocs:
        while i < len(text):
            end = text.find("\n", i)
            end = len(text) if end < 0 else end
            line = text[i:end]
            i = end + 1
            if (line.lstrip("\t") if strip_tabs else line) == delimiter:
                break
    return min(i, len(text))


def _simple_commands(command: str) -> Tuple[List[List[str]], bool]:
    """Split ``command`` into the simple commands the typing shell runs itself.

    Returns their words, quotes removed, and whether ``command`` has a comment.
    Subshells, command and process substitutions and heredoc bodies are
    skipped: nothing that runs there can end the shell the command was typed
    into. A tolerant scan, not a parser: it never raises, and input it reads
    wrong at worst puts a command in a subshell it did not need.
    """
    commands: List[List[str]] = [[]]
    word: List[str] = []
    in_word = False
    has_comment = False
    heredocs: List[Tuple[str, bool]] = []
    i, length = 0, len(command)

    def end_word() -> None:
        nonlocal in_word
        if in_word:
            commands[-1].append("".join(word))
            word.clear()
            in_word = False

    def end_command() -> None:
        end_word()
        if commands[-1]:
            commands.append([])

    while i < length:
        char = command[i]
        if char == "\\":
            if command.startswith("\\\n", i):
                i += 2
                continue
            word.append(command[i + 1 : i + 2])
            in_word = True
            i += 2
        elif char == "'":
            end = _skip_single_quoted(command, i + 1)
            word.append(command[i + 1 : end - 1])
            in_word = True
            i = end
        elif char == '"':
            end = _skip_double_quoted(command, i + 1)
            word.append(command[i + 1 : end - 1])
            in_word = True
            i = end
        elif command.startswith("$'", i):
            end = _skip_ansi_c_quoted(command, i + 2)
            word.append(command[i:end])
            in_word = True
            i = end
        elif command.startswith("$(", i):
            end = _skip_nested(command, i + 2, "(", ")")
            word.append(command[i:end])
            in_word = True
            i = end
        elif command.startswith("${", i):
            end = _skip_nested(command, i + 2, "{", "}")
            word.append(command[i:end])
            in_word = True
            i = end
        elif char == "`":
            end = _skip_backquoted(command, i + 1)
            word.append(command[i:end])
            in_word = True
            i = end
        elif char in "<>" and command.startswith("(", i + 1):
            # Process substitution runs in a subshell.
            end_word()
            i = _skip_nested(command, i + 2, "(", ")")
        elif char in "<>" and command.startswith("&", i + 1):
            # ``2>&1``: a redirection, not a background ``&``.
            word.append(command[i : i + 2])
            in_word = True
            i += 2
        elif command.startswith("&>", i):
            end_word()
            word.append("&>")
            in_word = True
            i += 2
        elif command.startswith("<<", i) and not command.startswith("<<<", i):
            end_word()
            i += 2
            strip_tabs = command.startswith("-", i)
            i += 1 if strip_tabs else 0
            while i < length and command[i] in " \t":
                i += 1
            delimiter, i = _read_heredoc_delimiter(command, i)
            if delimiter:
                heredocs.append((delimiter, strip_tabs))
        elif char == "#" and not in_word:
            has_comment = True
            end = command.find("\n", i)
            i = length if end < 0 else end
        elif char == "\n":
            end_command()
            i = _skip_heredoc_bodies(command, i + 1, heredocs)
            heredocs.clear()
        elif char in ";&|":
            end_command()
            i += 1
        elif char == "(":
            # A subshell (or arithmetic) in command position stands for one
            # command; elsewhere it belongs to the word (``f()``, ``a=(1 2)``).
            starts_command = not in_word and not commands[-1]
            end = _skip_nested(command, i + 1, "(", ")")
            word.append(command[i:end])
            in_word = True
            i = end
            if starts_command:
                end_word()
        elif char == ")":
            # Unbalanced at this level: a ``case`` pattern ends here.
            end_command()
            i += 1
        elif char in " \t":
            end_word()
            i += 1
        else:
            word.append(char)
            in_word = True
            i += 1
    end_command()
    return [words for words in commands if words], has_comment


def _sets_errexit(args: List[str]) -> bool:
    """True when ``set`` with ``args`` turns on ``errexit`` (``set -e``)."""
    remaining = iter(args)
    for arg in remaining:
        if arg == "--":
            return False
        if arg == "-o":
            if next(remaining, "") == "errexit":
                return True
        elif arg.startswith("-") and not arg.startswith("--") and "e" in arg[1:]:
            return True
    return False


def _ends_typing_shell(words: List[str]) -> bool:
    """True when the simple command ``words`` can end the shell running it."""
    start = 0
    if words[0] == "function" and len(words) > 1:
        start = 2
    while start < len(words) and (
        words[start] in _COMMAND_PREFIX_WORDS
        or _ASSIGNMENT_WORD.match(words[start])
        or _FUNCTION_HEAD.fullmatch(words[start])
    ):
        start += 1
    if start >= len(words):
        return False
    name, args = words[start], words[start + 1 :]
    if name in _SHELL_ENDING_BUILTINS:
        return True
    return name == "set" and _sets_errexit(args)


def ends_typing_shell(command: str) -> bool:
    """True when ``command``, typed into a shell, can end that shell.

    That is a top-level ``exit``, ``logout`` or ``exec``, or ``set -e``, which
    ends an interactive shell at the next failing command. The same words in a
    subshell, a substitution, quotes or a heredoc body do not count.
    """
    commands, _ = _simple_commands(command)
    return any(_ends_typing_shell(words) for words in commands)


def confine_shell_exit(command: str) -> str:
    """``command``, run in a subshell when it would otherwise end the tab.

    In the subshell an ``exit`` ends only that command and sets its exit code,
    and what follows it on the line does not run, as under ``bash -c``. The
    command's own ``cd`` and variables do not outlast it then, which is also
    what a model that writes ``exit`` expects. Anything else is returned
    unchanged.
    """
    commands, has_comment = _simple_commands(command)
    if not any(_ends_typing_shell(words) for words in commands):
        return command
    if "\n" in command or has_comment:
        # A trailing comment would swallow a ``)`` on the same line.
        return f"(\n{command}\n)"
    return f"( {command} )"


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
