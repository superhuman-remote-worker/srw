"""Persistent shell session manager — delegates to the workspace backend.

The agent's shells run ON THE WORKSPACE (container pod or VM), never in the
agent pod: ShellManager requires a workspace backend that declares
``supports_shell`` and forwards every operation to it (RemoteBackend drives
tmux on the workspace over SSH). The former local-libtmux execution path was
removed — it served a bare-metal dev posture that is deprecated, and a
dormant in-pod execution path is a liability (same rationale as
knowledge-base/knowledge/issues/remove_local_browser_fallback.md; see
knowledge-base/knowledge/features/no_workspace_agent_mode.md §9).

What remains agent-side:
  * sudo interception and blocked-command gating (must fire before any
    delegation so the backend path cannot bypass them),
  * compatibility exports of the sentinel/stall protocol owned by
    src/shared/runtime/core/shell_protocol.py and used directly by workspace transports.

Two tools expose this manager: shell_execute / run_command and shell_read.
"""

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from shared.runtime.core.shell_protocol import (
    build_sentinel_command as build_sentinel_command,
    COMMAND_TYPE_MAP as COMMAND_TYPE_MAP,
    SUDO_FREEZE_SENTINEL as SUDO_FREEZE_SENTINEL,
    DEFAULT_BLOCKED_COMMANDS as DEFAULT_BLOCKED_COMMANDS,
    INTERACTIVE_PROMPT_PATTERNS as INTERACTIVE_PROMPT_PATTERNS,
    NO_CHANGE_TIMEOUT_SECONDS as NO_CHANGE_TIMEOUT_SECONDS,
    HARD_TIMEOUT_CAP_SECONDS as HARD_TIMEOUT_CAP_SECONDS,
    NONINTERACTIVE_ENV_EXPORT as NONINTERACTIVE_ENV_EXPORT,
    STILL_RUNNING_TEMPLATE as STILL_RUNNING_TEMPLATE,
    STILL_RUNNING_HARDCAP_TEMPLATE as STILL_RUNNING_HARDCAP_TEMPLATE,
    COLLIDING_COMMAND_TEMPLATE as COLLIDING_COMMAND_TEMPLATE,
    INTERACTIVE_PROMPT_TEMPLATE as INTERACTIVE_PROMPT_TEMPLATE,
    compute_no_change_state as compute_no_change_state,
    prompt_is_ready as prompt_is_ready,
)

logger = logging.getLogger(__name__)


@dataclass
class ShellTab:
    """Agent-side stub tracking a tab that lives on the workspace backend."""

    name: str
    tab_type: str  # "shell", "ssh", "repl", "process"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_metadata(self) -> Dict[str, Any]:
        """Return a metadata dict for list_tabs."""
        return {
            "name": self.name,
            "type": self.tab_type,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }


class ShellManager:
    """Forwards persistent tmux-backed shell sessions to the workspace backend.

    Every operation delegates to a workspace backend that declares
    ``supports_shell`` (e.g. RemoteBackend, which owns the tmux session on
    the workspace over SSH). There is NO local execution path: constructing
    a ShellManager without a shell-capable backend raises, and the callers
    simply don't register shell tools for such workspaces (capability, not
    inference).

    Agent-side gating (sudo interception, blocked commands) runs before any
    delegation so the backend path cannot bypass it. Tab limits, scrollback
    and timeout semantics are enforced by the backend implementation; the
    corresponding constructor parameters are retained for config plumbing.
    """

    def __init__(
        self,
        job_id: str,
        max_tabs: int = 15,
        scrollback_limit: int = 5000,
        default_timeout: int = 120,
        no_change_timeout: float = NO_CHANGE_TIMEOUT_SECONDS,
        blocked_commands: Optional[List[str]] = None,
        sandbox_cwd: Optional[str] = None,
        backend: Optional[Any] = None,
        sudo_action: str = "freeze",
        sudo_block_message: Optional[str] = None,
    ):
        """Initialize ShellManager over a shell-capable workspace backend.

        Args:
            job_id: Unique job identifier (used in session name)
            max_tabs: Maximum number of concurrent shell tabs (backend-enforced)
            scrollback_limit: Tmux history-limit per pane (backend-enforced)
            default_timeout: Default (hard) timeout for run_sync in seconds
                             (backend-enforced)
            no_change_timeout: Seconds of no new output before a still-running
                               command yields control back (backend-enforced)
            blocked_commands: Commands to block (None = use defaults)
            sandbox_cwd: Working directory to restrict commands to (backend
                         workspaces start in the workspace root)
            backend: Workspace backend with shell support — REQUIRED. All
                     shell operations delegate to it.
            sudo_action: How to handle sudo commands. "freeze" returns a sentinel
                         for the tool layer to trigger a job freeze (VM upgrade prompt).
                         "block" hard-rejects. "allow" passes through (VM-backed agents).
            sudo_block_message: Optional custom message for the "block" action —
                         the orchestrator injects the operator's denial reason here
                         after a vm_upgrade request is denied, so the agent gets a
                         reasoned rejection instead of the generic block text.

        Raises:
            RuntimeError: If no backend is given or it does not declare shell
                support. The local (in-pod) libtmux execution path was removed —
                shells only run on the workspace, never in the agent pod.
        """
        if backend is None or not getattr(backend, "supports_shell", False):
            raise RuntimeError(
                "ShellManager requires a workspace backend with shell support "
                "(backend.supports_shell=True). Local in-pod tmux execution was "
                "removed — shells run only on the workspace. See "
                "knowledge-base/knowledge/features/no_workspace_agent_mode.md §9."
            )

        self.job_id = job_id
        self.max_tabs = max_tabs
        self.scrollback_limit = scrollback_limit
        self.default_timeout = default_timeout
        self.no_change_timeout = no_change_timeout
        self.sandbox_cwd = sandbox_cwd
        self._backend = backend
        self.sudo_action = sudo_action
        self.sudo_block_message = sudo_block_message

        if blocked_commands is None:
            self.blocked_commands = DEFAULT_BLOCKED_COMMANDS
        else:
            self.blocked_commands = frozenset(blocked_commands)

        # Agent-side stubs mirroring backend tabs (bookkeeping only — the
        # backend owns the authoritative tab state).
        self._tabs: OrderedDict[str, ShellTab] = OrderedDict()
        self._session_name = f"agent_{job_id[:12]}"
        logger.info(
            f"ShellManager initialized with backend delegation: "
            f"session={self._session_name}"
        )

    def ensure_tab(self, name: str) -> ShellTab:
        """Get an existing tab or auto-create a new shell tab on the backend.

        Args:
            name: Tab name (lowercase alphanumeric + hyphens, max 20 chars)

        Returns:
            ShellTab stub (existing or newly created)

        Raises:
            ValueError: If name is invalid (backend-validated)
        """
        self._backend.shell_ensure_tab(name)
        if name not in self._tabs:
            self._tabs[name] = ShellTab(name=name, tab_type="shell")
        return self._tabs[name]

    def open_tab(
        self,
        name: str,
        command: Optional[str] = None,
        tab_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Open a new named tab in the backend's tmux session.

        Args:
            name: Tab name (lowercase alphanumeric + hyphens, max 20 chars)
            command: Optional command to run on tab creation
            tab_type: Tab type ("shell", "ssh", "repl", "process").
                      Auto-detected from command if not specified.

        Returns:
            Metadata dict for the new tab

        Raises:
            ValueError: If name is invalid or duplicate (backend-validated)
        """
        metadata = self._backend.shell_open_tab(
            name, command=command, tab_type=tab_type
        )
        self._tabs[name] = ShellTab(
            name=name,
            tab_type=metadata.get("type", "shell"),
        )
        return metadata

    def send(
        self,
        name: str,
        text: str,
        enter: bool = True,
        working_dir: Optional[str] = None,
        allow_busy: bool = False,
    ) -> str:
        """Send keystrokes to a tab.

        Args:
            name: Tab name
            text: Text to send (plain text or tmux key names like "Up", "C-c")
            enter: Whether to press Enter after sending (default True)
            working_dir: Optional workspace-relative directory for an async
                         command. The backend restores the workspace root when
                         that command exits. Do not use for raw keystrokes.
            allow_busy: Permit input to an existing foreground process. Set by
                        explicit keys mode only; async commands must keep this
                        false so they cannot collide with prior work.

        Returns:
            Confirmation message or blocked error

        Raises:
            KeyError: If tab doesn't exist
        """
        # Check blocked commands when actually executing (enter=True).
        # Must run before backend delegation so sudo intercept always fires.
        if enter:
            blocked = self._check_blocked(text)
            if blocked:
                return blocked
        return self._backend.shell_send(
            name,
            text,
            enter=enter,
            working_dir=working_dir,
            allow_busy=allow_busy,
        )

    def cancel(self, name: str = "default") -> str:
        """Send Ctrl+C to a tab to abort a stuck/hung command.

        Delegates to the backend's C-c ladder (interrupt, retry, then tab
        reset). Backing method for the stateless ``cancel_command`` tool.
        """
        return self._backend.shell_cancel(name)

    def read(
        self,
        name: str,
        lines: int = 50,
        since_cursor: bool = False,
    ) -> Tuple[str, Dict[str, Any]]:
        """Read output from a tab's terminal buffer.

        Args:
            name: Tab name
            lines: Number of lines to read from the end (default 50)
            since_cursor: If True, read only lines added since last read

        Returns:
            Tuple of (text, metadata_dict)

        Raises:
            KeyError: If tab doesn't exist
        """
        return self._backend.shell_read(name, lines=lines, since_cursor=since_cursor)

    def close_tab(self, name: str) -> str:
        """Close a tab by killing its tmux window on the backend.

        Args:
            name: Tab name

        Returns:
            Confirmation message

        Raises:
            KeyError: If tab doesn't exist
        """
        result = self._backend.shell_close_tab(name)
        self._tabs.pop(name, None)
        return result

    def read_with_offset(
        self,
        name: str,
        lines: int = 30,
        offset: Optional[int] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Read output from a tab with optional absolute offset.

        Reads terminal scrollback like a file. When offset is provided,
        reads `lines` lines starting from that position. When offset is
        None, reads the last `lines` lines (tail behavior).

        Args:
            name: Tab name
            lines: Number of lines to return (default 30)
            offset: Absolute line position to start from (0 = start of scrollback).
                    If None, reads from the end (tail behavior).

        Returns:
            Tuple of (text, metadata_dict)

        Raises:
            KeyError: If tab doesn't exist
        """
        return self._backend.shell_read_with_offset(name, lines=lines, offset=offset)

    def format_tab_header(self) -> str:
        """Build tab header string showing all live shells.

        Returns:
            Header string like ``[Shells: default | gpu-box]``
        """
        return self._backend.shell_format_tab_header()

    def take_disk_full_notice(self) -> Optional[str]:
        """The backend's one-time notice that the workspace disk is full."""
        take = getattr(self._backend, "take_disk_full_notice", None)
        return take() if callable(take) else None

    def list_tabs(self) -> List[Dict[str, Any]]:
        """Return metadata for all tabs.

        Returns:
            List of tab metadata dicts
        """
        return self._backend.shell_list_tabs()

    def run_sync(
        self,
        command: str,
        timeout: Optional[int] = None,
        working_dir: Optional[str] = None,
        tab_name: str = "default",
    ) -> str:
        """Execute a command synchronously in a shell-type tab on the backend.

        The backend uses a sentinel marker pattern to detect command
        completion and extract the exit code; it derives the soft (no-change)
        and hard timeouts from ``timeout`` exactly as documented on the
        module constants (an explicit timeout disables the soft no-change
        timeout).

        Args:
            command: Shell command to execute
            timeout: Timeout in seconds (None => backend default + soft timeout)
            working_dir: Working directory for the command (relative to workspace)
            tab_name: Name of the tab to run in (default "default"). Must be
                      a shell-type tab (sentinel detection requires a bash-like shell).

        Returns:
            Formatted output string with exit code and stdout

        Raises:
            ValueError: If command is blocked or tab is not shell-type
            KeyError: If tab does not exist
        """
        # Safety check — must run before backend delegation so sudo
        # intercept and blocked-command checks always fire.
        blocked = self._check_blocked(command)
        if blocked:
            return blocked

        return self._backend.shell_run(
            command,
            timeout=timeout,
            tab_name=tab_name,
            working_dir=working_dir,
        )

    def cleanup(self, *, strict: bool = False) -> None:
        """Kill the backend tmux session and clear local tab stubs."""
        if strict:
            cleanup = getattr(self._backend, "shell_cleanup_strict", None)
            if not callable(cleanup):
                raise RuntimeError("backend lacks strict shell cleanup")
            cleanup()
        else:
            self._backend.shell_cleanup()
        self._tabs.clear()

    def is_alive(self) -> bool:
        """Check if the backend tmux session still exists."""
        return self._backend.shell_is_alive()

    def _check_blocked(self, command: str) -> str | None:
        """Return error message if command's first word is blocked, else None.

        Returns SUDO_FREEZE_SENTINEL when sudo_action is "freeze" and the
        command starts with sudo. The tool layer detects this sentinel and
        triggers a job freeze (VM upgrade prompt).
        """
        first_word = command.strip().split()[0] if command.strip() else ""
        if not first_word:
            return None

        # Sudo intercept (separate from blocked_commands)
        if first_word == "sudo":
            if self.sudo_action == "allow":
                return None  # VM-backed agents: pass through
            elif self.sudo_action == "freeze":
                return SUDO_FREEZE_SENTINEL
            else:  # "block"
                return self.sudo_block_message or (
                    "Command blocked: 'sudo' is not available in this container. "
                    "System package installation requires a VM runtime."
                )

        # Standard blocked commands
        if self.blocked_commands and first_word in self.blocked_commands:
            return (
                f"Command blocked: '{first_word}' is not allowed. "
                f"Blocked commands: {', '.join(sorted(self.blocked_commands))}"
            )
        return None
