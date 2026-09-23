"""Shell tools for the Universal Agent.

Provides tools for command execution in two modes (configured via shell.mode):

Stateless mode (default):
- run_command: Simple command→output execution (hidden persistent tab underneath)
- shell_read: Read more output from scrollback when needed

Persistent mode (opt-in):
- shell_execute: Full tab management, keystrokes, async commands
- shell_read: Read output from any named terminal tab

Available in both strategic and tactical phases.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional

from langchain_core.tools import tool

from agent.core.tool_output_redaction import workspace_secrets
from agent.tools.shell.coding_tools import _truncate_output
from agent.tools.shell.shell_manager import SUDO_FREEZE_SENTINEL
from agent.tools.context import ToolContext
from agent.services.cloud_mount.guardrails import (
    command_may_write_cloud,
    command_touches_cloud_mount,
    detect_cloud_delete_risk,
    detect_cloud_scan_risk,
    format_cloud_delete_guard_message,
    format_cloud_scan_guard_message,
)

from shared.tool_catalog.definitions import (
    SHELL_TOOLS_METADATA as SHELL_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)

# Default max output characters for shell reads
DEFAULT_MAX_READ_LINES = 200
DEFAULT_MAX_OUTPUT_CHARS = 50000

# Error patterns to scan for in shell output (P11 mitigation)
# These indicate application-level errors even when exit code is 0
_SHELL_ERROR_PATTERNS = [
    ("Traceback (most recent call last)", "Python traceback"),
    ("PermissionError:", "Permission denied"),
    ("ConnectionRefusedError:", "Connection refused"),
    ("FileNotFoundError:", "File not found"),
    ("ModuleNotFoundError:", "Missing Python module"),
    ("OSError:", "OS error"),
    ("FATAL:", "Fatal error"),
    ("panic:", "Go/Rust panic"),
    ("segmentation fault", "Segfault"),
    ("killed", "Process killed (OOM?)"),
    ("No space left on device", "Disk full"),
    ("Connection timed out", "Connection timeout"),
    ("Name or service not known", "DNS resolution failed"),
]


def _scan_for_error_patterns(output: str) -> Optional[str]:
    """Scan shell output for known error patterns and return a warning if found."""
    output_lower = output.lower()
    found = []
    for pattern, label in _SHELL_ERROR_PATTERNS:
        if pattern.lower() in output_lower:
            found.append(label)
    if found:
        return f"⚠ Possible error in output: {', '.join(found)}. Read the output carefully before proceeding."
    return None


def _cloud_scan_guard_decision(
    command: str, context: ToolContext
) -> tuple[Optional[str], bool]:
    cloud_mount_cfg = context.get_config("cloud_mount", {})
    if not isinstance(cloud_mount_cfg, dict) or not cloud_mount_cfg.get("active"):
        return None, False

    mode = str(
        cloud_mount_cfg.get("scan_guard", os.getenv("SRW_CLOUD_SCAN_GUARD", "block"))
    ).lower()
    if mode in {"0", "off", "disabled", "false"}:
        return None, False

    risk = detect_cloud_scan_risk(command)
    if risk is None:
        return None, False

    message = format_cloud_scan_guard_message(command, risk)
    if mode == "warn":
        return (
            f"{message}\n\nThe command will still run because cloud_scan_guard=warn.",
            False,
        )
    return message, True


def _cloud_delete_guard_decision(
    command: str, context: ToolContext
) -> tuple[Optional[str], bool]:
    cloud_mount_cfg = context.get_config("cloud_mount", {})
    if not isinstance(cloud_mount_cfg, dict) or not cloud_mount_cfg.get("active"):
        return None, False

    mode = str(
        cloud_mount_cfg.get("scan_guard", os.getenv("SRW_CLOUD_SCAN_GUARD", "block"))
    ).lower()
    if mode in {"0", "off", "disabled", "false"}:
        return None, False

    risk = detect_cloud_delete_risk(command)
    if risk is None:
        return None, False

    message = format_cloud_delete_guard_message(
        command, risk, protected=bool(cloud_mount_cfg.get("protected"))
    )
    if mode == "warn":
        return (
            f"{message}\n\nThe command will still run because cloud_scan_guard=warn.",
            False,
        )
    return message, True


def _cloud_cache_guard_decision(command: str, context: ToolContext) -> Optional[str]:
    cloud_mount_cfg = context.get_config("cloud_mount", {})
    if not isinstance(cloud_mount_cfg, dict) or not cloud_mount_cfg.get("active"):
        return None
    if not command_touches_cloud_mount(command):
        return None
    manager = cloud_mount_cfg.get("_manager")
    if manager is None or not hasattr(manager, "cache_limit_message"):
        return None
    try:
        return manager.cache_limit_message()
    except Exception as exc:
        logger.warning("Cloud cache guard check failed: %s", exc)
        return None


def _cloud_upperdir_guard_decision(command: str, context: ToolContext) -> Optional[str]:
    cloud_mount_cfg = context.get_config("cloud_mount", {})
    if not isinstance(cloud_mount_cfg, dict) or not cloud_mount_cfg.get("active"):
        return None
    if not command_may_write_cloud(command):
        return None
    if not command_touches_cloud_mount(command):
        return None
    overlay = cloud_mount_cfg.get("_overlay_manager")
    if overlay is None or not hasattr(overlay, "quota_guard_message"):
        return None
    try:
        return overlay.quota_guard_message()
    except Exception as exc:
        logger.warning("Cloud upperdir guard check failed: %s", exc)
        return None


def _cloud_mount_manager(context: ToolContext) -> Any | None:
    cloud_mount_cfg = context.get_config("cloud_mount", {})
    if not isinstance(cloud_mount_cfg, dict) or not cloud_mount_cfg.get("active"):
        return None
    manager = cloud_mount_cfg.get("_manager")
    if manager is None or not hasattr(manager, "status"):
        return None
    return manager


# Tmux special key names that should NOT get Enter appended in keys mode
TMUX_SPECIAL_KEYS = frozenset(
    {
        "Up",
        "Down",
        "Left",
        "Right",
        "Enter",
        "Tab",
        "Escape",
        "Space",
        "BSpace",
        "Home",
        "End",
        "PageUp",
        "PageDown",
        "NPage",
        "PPage",
        "F1",
        "F2",
        "F3",
        "F4",
        "F5",
        "F6",
        "F7",
        "F8",
        "F9",
        "F10",
        "F11",
        "F12",
        "IC",
        "DC",  # Insert, Delete
    }
)

# Tool metadata for registry
# Appended to run_command results when its shared command tab is wedged
# (still-running, colliding, or blocked on a prompt). run_command has no keys
# mode, so cancel_command is the model's only in-band escape from a hung tab.
_CANCEL_HINT = (
    "\n\nNote: if this command is stuck or you did not expect it to run this "
    "long, call cancel_command to abort it and free the tab."
)


def _check_sudo_freeze(
    output: str, command: str, context: ToolContext
) -> Optional[str]:
    """If output is the sudo freeze sentinel, trigger a job freeze and return
    a message for the agent. Returns None if not a sudo freeze."""
    if output != SUDO_FREEZE_SENTINEL:
        return None
    context.request_freeze(
        {
            "freeze_type": "vm_upgrade_required",
            "reason": "Agent attempted a sudo command that requires VM-level access.",
            "command": command,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    return (
        "This command requires elevated privileges (sudo). "
        "The job has been paused while the operator decides whether to "
        "upgrade this job to a VM environment. You do not need to take "
        "any action — the job will resume automatically if approved."
    )


def _apply_tail(output: str, tail: int) -> str:
    """Apply tail truncation to run_sync output, preserving the header.

    run_sync returns formats like:
        Exit code: 0
        --- stdout ---
        line1
        ...

    Or on timeout/interactive detection:
        Command timed out after 120s: ssh admin@host
        --- terminal state ---
        Are you sure you want to continue connecting (yes/no)?

    This function keeps the header and truncates only the body.
    """
    lines = output.split("\n")

    # Find the separator line (either stdout or terminal state)
    separator_idx = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped in ("--- stdout ---", "--- terminal state ---"):
            separator_idx = i
            break

    if separator_idx is None:
        # No separator (e.g. "(no output)" or short messages)
        return output

    header = lines[: separator_idx + 1]  # "Exit code: N" + "--- stdout ---"
    body = lines[separator_idx + 1 :]

    if len(body) <= tail:
        return output

    truncated_body = body[-tail:]
    skipped = len(body) - tail
    return (
        "\n".join(header)
        + f"\n[...{skipped} lines truncated...]\n"
        + "\n".join(truncated_body)
    )


def _resolved_shell_mode(requested_names: Optional[Iterable[str]]) -> Optional[str]:
    """The mode the resolved name list already chose, or ``None``.

    ``get_all_tool_names`` is the one resolution of ``shell.mode``: it aliases
    ``run_command`` and ``shell_execute`` onto the mode's executor, so a
    resolved list names exactly one of them. Re-deriving the mode here from a
    second config read let the two disagree and bind neither. Anything else
    (no executor, both, a caller that passes no names) leaves it to config.
    """
    if requested_names is None:
        return None
    names = set(requested_names)
    if "shell_execute" in names and "run_command" not in names:
        return "persistent"
    if "run_command" in names and "shell_execute" not in names:
        return "stateless"
    return None


def create_shell_tools(
    context: ToolContext, requested_names: Optional[Iterable[str]] = None
) -> List[Any]:
    """Create shell tools with injected context.

    Returns different tool sets based on shell.mode:
    - "stateless" (default): [run_command, cancel_command, shell_read]
    - "persistent": [shell_execute, shell_read]

    Args:
        context: ToolContext with shell_manager
        requested_names: The resolved names ``load_tools`` is binding. When
            they name exactly one executor, that decides the mode.

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If shell_manager not available in context
    """
    sm = context.shell_manager
    if sm is None:
        raise ValueError("Shell tools require shell_manager in ToolContext")

    max_output_chars = context.get_config("max_output_chars", DEFAULT_MAX_OUTPUT_CHARS)
    max_read_lines = context.get_config("shell_max_read_lines", DEFAULT_MAX_READ_LINES)

    # Determine shell mode: the resolved name list, else config
    mode = _resolved_shell_mode(requested_names)
    if mode is None:
        shell_config = context.get_config("shell", {})
        mode = (
            shell_config.get("mode", "stateless")
            if isinstance(shell_config, dict)
            else "stateless"
        )

    @tool
    def srw_cloud_status() -> str:
        """Show rclone cloud mount status and cache usage.

        Use this before broad cloud work or when a cloud command is blocked by
        the cache guard. The output includes mount paths, current VFS cache
        usage, hard cache limits, and rclone RC status when available.
        """
        manager = _cloud_mount_manager(context)
        if manager is None:
            return "No active rclone cloud mount for this session."
        return manager.status()

    @tool
    def run_command(
        command: str,
        timeout: Optional[int] = None,
        tail: int = 30,
        working_dir: Optional[str] = ".",
    ) -> str:
        """Execute a shell command and return its output.

        Runs the command to completion and returns the exit code + stdout.
        Every call starts in `working_dir`, resolved relative to the workspace
        root, and restores the tab to the workspace root after completion.
        Always set `working_dir`. Do not use `cd` unless absolutely necessary;
        explicit working directories keep shell and file-tool paths aligned.
        Use for: running tests, building projects, git operations, file system
        commands, deploying, checking logs, SSH via sshpass, and any other
        shell task.

        LONG-RUNNING / QUIET COMMANDS: a command that produces no new output for
        ~30s (a large `pip install`, a build, a big download, data ingestion or
        embedding) is reported as "still running" with `Exit code: -1` — this is
        NOT an error; the command keeps running on the tab.
          - Best: when you already expect a command to be slow or quiet, pass an
            explicit `timeout` (e.g. 300-600) up front, so THIS call waits for
            the full duration and returns the real exit code.
          - If you do get "still running", call shell_read() once after a short
            wait to check progress — do NOT poll in a tight loop. The tab is busy
            until the command finishes, so re-issuing it (or any other command)
            is rejected. If the output stays completely unchanged for a long
            time, the command may be stuck — call cancel_command to abort it and
            free the tab, rather than polling forever.

        INTERACTIVE INPUT: run_command cannot answer prompts (password, y/n).
        Use a non-interactive form instead:
          - SSH: sshpass -p 'pass' ssh -o StrictHostKeyChecking=no user@host "cmd"
          - apt/dnf: use -y flag
          - git: configure credential helper

        SUDO NOTE: Commands prefixed with `sudo` may pause the job while
        the operator decides whether to upgrade to a VM environment. If the
        job is upgraded, it will resume automatically with sudo access. If
        not upgraded, try an alternative approach (pip install as user,
        compile from source in userspace, etc.).

        For long output, only the last `tail` lines are returned. Use
        shell_read() to page through the full scrollback if needed.

        Args:
            command: Shell command to execute (e.g., "pytest tests/ -x",
                "git status", "curl -s https://api.example.com/health").
            working_dir: Directory in which to start the command, relative to
                the workspace root (default ".", the workspace root). A null or
                empty value is also normalized to the workspace root.
            timeout: Max seconds to wait (max 600). Omit for normal commands —
                a quiet command then returns "still running" after ~30s. Set an
                explicit value for known long/quiet work (installs, builds) to
                wait the full duration without a no-change interruption, or for
                sudo commands (approval may take minutes).
            tail: Max stdout lines to return (default 30). Increase for
                verbose output (test suites, builds, logs).

        Returns:
            Exit code + stdout output (last `tail` lines), or error message.
        """
        try:
            sm.ensure_tab("default")

            cache_guard_msg = _cloud_cache_guard_decision(command, context)
            if cache_guard_msg:
                return cache_guard_msg

            upperdir_guard_msg = _cloud_upperdir_guard_decision(command, context)
            if upperdir_guard_msg:
                return upperdir_guard_msg

            guard_msg, guard_blocks = _cloud_scan_guard_decision(command, context)
            if guard_msg and guard_blocks:
                return guard_msg

            delete_msg, delete_blocks = _cloud_delete_guard_decision(command, context)
            if delete_msg and delete_blocks:
                return delete_msg

            output = sm.run_sync(
                command,
                tab_name="default",
                timeout=timeout,
                working_dir=working_dir or ".",
            )
            if guard_msg:
                output = f"{guard_msg}\n\n{output}"
            if delete_msg:
                output = f"{delete_msg}\n\n{output}"

            # Sudo intercept: trigger freeze for VM upgrade
            freeze_msg = _check_sudo_freeze(output, command, context)
            if freeze_msg:
                return freeze_msg

            # Still-running (soft/hard no-change timeout) is NOT an error — the
            # command keeps running. Pass it through so the model can poll with
            # shell_read, re-run with a higher timeout, or cancel_command it.
            if "--- still running ---" in output:
                output = _apply_tail(output, tail)
                output = _truncate_output(
                    output,
                    max_output_chars,
                    "output",
                    secrets=workspace_secrets(context),
                )
                return output + _CANCEL_HINT

            # Colliding: the shared run_command tab is busy with a previous
            # command, so this one never ran. cancel_command is the way out.
            if "previous command still running" in output:
                output = _truncate_output(
                    output,
                    max_output_chars,
                    "output",
                    secrets=workspace_secrets(context),
                )
                return output + _CANCEL_HINT

            # Genuine interactive prompt: stateless run_command can't answer it,
            # so steer the model toward a non-interactive form (or cancel it).
            if "Interactive prompt detected" in output:
                return (
                    f"Error: Command requires interactive input, which "
                    f"run_command cannot provide.\n"
                    f"Use a non-interactive form instead (sshpass, -y flags, "
                    f"`yes |`, etc.).\n"
                    f"{output}"
                    f"{_CANCEL_HINT}"
                )

            output = _apply_tail(output, tail)
            output = _truncate_output(
                output, max_output_chars, "output", secrets=workspace_secrets(context)
            )

            warning = _scan_for_error_patterns(output)
            if warning:
                return f"{warning}\n{output}"
            return output

        except (ValueError, KeyError, TimeoutError) as e:
            return f"Error: {e}"

    @tool
    def shell_execute(
        command: str,
        name: str = "default",
        tail: int = 30,
        is_async: bool = False,
        keys: bool = False,
        timeout: Optional[int] = None,
        working_dir: Optional[str] = None,
    ) -> str:
        """Execute a command or send keystrokes in a persistent terminal tab.

        Each tab is an independent shell with its own PTY (like a terminal
        window). Tabs auto-create on first use and persist between calls —
        environment, working directory, virtualenvs, and history all survive.
        You can SSH in one tab while running local commands in another.

        Always set `working_dir` for commands. Do not use `cd` unless absolutely
        necessary — when `working_dir` is omitted, an inline `cd` persists in
        that tab for the rest of the job. A supplied `working_dir` is resolved
        relative to the workspace root and the root is restored when the
        command finishes.

        IMPORTANT: Never write paramiko, fabric, pexpect, or subprocess SSH
        scripts. Every tab is a real terminal — just use ssh/scp/rsync directly.

        Modes:
          Default (sync): Runs the command and waits for it to finish. Returns
              exit code + stdout. If it produces no new output for ~30s it
              returns "still running" (Exit code: -1) — not an error; the command
              keeps running on the tab. Then you can: poll with shell_read (once
              after a wait, NOT in a tight loop), stop it with keys=True "C-c",
              run other work on a different `name` tab, or — best for work you
              expect to be slow/quiet — re-run it with an explicit `timeout`
              (e.g. 600) so the call waits the full duration. A tab with a
              still-running command rejects new commands until it finishes. If
              the command hits an interactive prompt (password, y/n, host key,
              etc.), it returns early with the prompt text so you can respond
              with keys=True. After responding, the tab works normally again.
          keys=True: Send raw keystrokes. Text input (passwords, "yes", "y")
              auto-submits with Enter. Control keys ("C-c", "Up", "Escape")
              are sent as-is. Send "Enter" alone to press Enter without text.
              `working_dir` is ignored because keystrokes target the live
              process exactly where it is already running.
          is_async=True: Fire-and-forget. Returns immediately without waiting.
              ONLY for long-running background processes (dev servers, builds,
              VPN connections). Use shell_read() later to check progress. When
              `working_dir` is set, the command is anchored there and the tab
              returns to the workspace root after the command exits.

        SSH workflow (use sync mode, not is_async):
          1. shell_execute(command="ssh user@host", name="srv")
               → detects password prompt, returns terminal state
          2. shell_execute(command="the_password", name="srv", keys=True)
               → password sent + Enter, SSH connects
          3. shell_execute(command="hostname", name="srv")
               → runs on the remote host via sync mode (returns exit code)
          4. shell_execute(command="exit", name="srv", keys=True)
               → closes SSH session, tab returns to local shell

        Args:
            command: Shell command to run, or keystroke when keys=True.
                Keys: "C-c", "C-d", "C-z", "C-l", "Up", "Down", "Enter",
                "Tab", "Escape", or literal text (passwords, "yes", "y").
            name: Tab name (auto-created if new). Lowercase + hyphens, max 20
                chars. Use descriptive names: "gpu-box", "build", "db-server".
            tail: Max stdout lines to return (default 30). Increase for
                verbose output (test suites, builds, logs).
            is_async: Don't wait for completion — return immediately. Only for
                long-running background processes. NOT needed for SSH.
            keys: Send raw keystrokes. Text auto-submits with Enter;
                control keys (C-c, Up, Escape, etc.) are sent as-is.
            timeout: Max seconds to wait in sync mode (max 600). Omit for the
                default no-change behavior (~30s quiet → "still running"); set
                an explicit value for known long/quiet work (large installs,
                builds) to wait the full duration.
            working_dir: Directory in which to start the command, relative to
                the workspace root. In sync and async command modes the tab is
                restored to the workspace root after completion. Ignored when
                `keys=True`.

        Returns:
            [Shells: tab1 | tab2 | ...] header + command output.
        """
        try:
            # Ensure tab exists (auto-create if needed)
            sm.ensure_tab(name)
            tab_header = sm.format_tab_header()

            if keys:
                # Keys mode: text input auto-submits with Enter, control keys sent as-is
                is_special = command in TMUX_SPECIAL_KEYS or command.startswith(
                    ("C-", "M-")
                )
                result = sm.send(
                    name,
                    command,
                    enter=not is_special,
                    allow_busy=True,
                )
                # Sudo intercept: trigger freeze for VM upgrade
                freeze_msg = _check_sudo_freeze(result, command, context)
                if freeze_msg:
                    return freeze_msg
                time.sleep(0.5)
                text, metadata = sm.read_with_offset(name, lines=tail)
                text = _truncate_output(
                    text,
                    max_output_chars,
                    "shell output",
                    secrets=workspace_secrets(context),
                )
                return f"{tab_header}\n{text}"

            elif is_async:
                cache_guard_msg = _cloud_cache_guard_decision(command, context)
                if cache_guard_msg:
                    return f"{tab_header}\n{cache_guard_msg}"

                upperdir_guard_msg = _cloud_upperdir_guard_decision(command, context)
                if upperdir_guard_msg:
                    return f"{tab_header}\n{upperdir_guard_msg}"

                guard_msg, guard_blocks = _cloud_scan_guard_decision(command, context)
                if guard_msg and guard_blocks:
                    return f"{tab_header}\n{guard_msg}"

                delete_msg, delete_blocks = _cloud_delete_guard_decision(
                    command, context
                )
                if delete_msg and delete_blocks:
                    return f"{tab_header}\n{delete_msg}"
                # Async mode: send command, wait briefly, return what appeared
                sm.read(name, lines=1, since_cursor=False)  # snapshot cursor
                result = sm.send(
                    name,
                    command,
                    enter=True,
                    working_dir=working_dir,
                )
                # Sudo intercept: trigger freeze for VM upgrade
                freeze_msg = _check_sudo_freeze(result, command, context)
                if freeze_msg:
                    return freeze_msg
                time.sleep(0.5)
                text, metadata = sm.read(name, since_cursor=True)
                text = _truncate_output(
                    text,
                    max_output_chars,
                    "shell output",
                    secrets=workspace_secrets(context),
                )
                if guard_msg:
                    text = f"{guard_msg}\n\n{text}"
                if delete_msg:
                    text = f"{delete_msg}\n\n{text}"
                return f"{tab_header}\n{text}"

            else:
                cache_guard_msg = _cloud_cache_guard_decision(command, context)
                if cache_guard_msg:
                    return f"{tab_header}\n{cache_guard_msg}"

                upperdir_guard_msg = _cloud_upperdir_guard_decision(command, context)
                if upperdir_guard_msg:
                    return f"{tab_header}\n{upperdir_guard_msg}"

                guard_msg, guard_blocks = _cloud_scan_guard_decision(command, context)
                if guard_msg and guard_blocks:
                    return f"{tab_header}\n{guard_msg}"

                delete_msg, delete_blocks = _cloud_delete_guard_decision(
                    command, context
                )
                if delete_msg and delete_blocks:
                    return f"{tab_header}\n{delete_msg}"
                # Sync mode: sentinel-based wait for completion
                output = sm.run_sync(
                    command,
                    tab_name=name,
                    timeout=timeout,
                    working_dir=working_dir,
                )
                if guard_msg:
                    output = f"{guard_msg}\n\n{output}"
                if delete_msg:
                    output = f"{delete_msg}\n\n{output}"
                # Sudo intercept: trigger freeze for VM upgrade
                freeze_msg = _check_sudo_freeze(output, command, context)
                if freeze_msg:
                    return freeze_msg
                output = _apply_tail(output, tail)
                output = _truncate_output(
                    output,
                    max_output_chars,
                    "output",
                    secrets=workspace_secrets(context),
                )
                # Scan for application-level errors in output
                warning = _scan_for_error_patterns(output)
                if warning:
                    return f"{tab_header}\n{warning}\n{output}"
                return f"{tab_header}\n{output}"

        except (ValueError, KeyError, TimeoutError) as e:
            try:
                tab_header = sm.format_tab_header()
            except Exception:
                tab_header = "[Shells: ?]"
            return f"{tab_header}\nError: {e}"

    @tool
    def shell_read(
        name: str = "default",
        offset: Optional[int] = None,
        lines: int = 30,
    ) -> str:
        """Read output from a persistent terminal tab.

        Returns terminal scrollback from the named tab. Use after is_async
        commands to check on long-running processes, or to inspect a tab's
        full history. Omit offset to read from the end (tail), or set
        offset=0 to read from the start like a file.

        Args:
            name: Tab name (default "default").
            offset: Line position to start from (0 = start of scrollback).
                If omitted, reads from the end (tail behavior).
            lines: Number of lines to return (default 30, max 200).

        Returns:
            Tab header + terminal output with line count metadata.
        """
        try:
            capped_lines = min(lines, max_read_lines)
            sm.ensure_tab(name)
            tab_header = sm.format_tab_header()

            text, metadata = sm.read_with_offset(
                name, lines=capped_lines, offset=offset
            )
            text = _truncate_output(
                text,
                max_output_chars,
                "shell output",
                secrets=workspace_secrets(context),
            )
            info = f"({metadata['mode']}) {metadata['lines_returned']}/{metadata['total_lines']} lines"
            return f"{tab_header}\n{info}\n{text}"
        except (KeyError, ValueError) as e:
            try:
                tab_header = sm.format_tab_header()
            except Exception:
                tab_header = "[Shells: ?]"
            return f"{tab_header}\nError: {e}"

    @tool
    def cancel_command() -> str:
        """Abort a stuck or hung command by sending Ctrl+C to the shell.

        Use this when a run_command reported "still running" and you did NOT
        expect the command to be long, when a command is hung / not returning,
        or when the tab is stuck on an interactive prompt you cannot answer.
        run_command uses a single shell tab, so a wedged command blocks every
        later command (including your own recovery attempts) until it is
        cancelled — this is how you get unstuck instead of giving up.

        Sends Ctrl+C to the tab (retrying once); if the process ignores the
        interrupt, resets the tab as a last resort. Afterwards the tab is free
        to run new commands. Safe to call when nothing is running (it says so).

        Returns:
            A short status: whether the command was interrupted, there was
            nothing to cancel, or the tab had to be reset.
        """
        try:
            sm.ensure_tab("default")
            return sm.cancel("default")
        except (ValueError, KeyError) as e:
            return f"Error: {e}"

    if mode == "persistent":
        return [shell_execute, shell_read, srw_cloud_status]
    else:
        # Stateless mode (default)
        return [run_command, cancel_command, shell_read, srw_cloud_status]
