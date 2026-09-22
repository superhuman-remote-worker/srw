"""Abstract workspace backend interface.

Defines the contract for workspace storage backends. All file paths are
relative to the workspace root. Implementations handle the transport
(local filesystem, SSH/SFTP, etc.) while managers provide higher-level logic.

See knowledge-base/knowledge/features/vm_backend.md for the full design.
"""

from abc import ABC, abstractmethod
from fnmatch import fnmatchcase
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Tuple


class WorkspaceUnavailableError(Exception):
    """Raised when the workspace backend cannot be reached.

    For RemoteBackend: SSH connection lost, VM unreachable, etc.
    The agent should report this to the orchestrator for VM recovery.
    """

    pass


class WorkspaceAuthenticationError(Exception):
    """Raised when workspace SSH credentials/configuration are unusable.

    Unlike ``WorkspaceUnavailableError``, this is deterministic and terminal:
    recreating the same pod with the same key will not repair a missing,
    unreadable, invalid, or unauthorized private key.
    """

    pass


# --- transient infrastructure classification --------------------------------
#
# Matching is by exception CLASS NAME across the MRO rather than by importing
# driver exception trees. The agent and orchestrator images do not carry the
# same drivers (psycopg rides with the LangGraph checkpointer, asyncpg with the
# orchestrator), so a hard import would either crash on the wrong image or force
# a try/except ladder that silently degrades. Name matching is stringly-typed,
# but the deny-list below is checked FIRST, so the failure mode of an accidental
# match is "stays terminal", never "a real bug becomes an infinite retry".
#
# knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md (Defect 1)

# Real bugs. These must never be retried — retrying a constraint violation or a
# missing column just burns the attempt ceiling and hides the defect.
_NEVER_TRANSIENT_EXC_NAMES = frozenset(
    {
        "CheckViolationError",
        "UniqueViolationError",
        "ForeignKeyViolationError",
        "NotNullViolationError",
        "IntegrityConstraintViolationError",
        "IntegrityError",
        "UndefinedColumnError",
        "UndefinedTableError",
        "UndefinedFunctionError",
        "UndefinedObjectError",
        "InvalidTextRepresentationError",
        "DatatypeMismatchError",
        "SyntaxOrAccessError",
        "ProgrammingError",
        "DataError",
    }
)

# Unambiguously a lost/unavailable connection whatever the message says.
_ALWAYS_TRANSIENT_EXC_NAMES = frozenset(
    {
        # asyncpg
        "ConnectionDoesNotExistError",
        "ConnectionFailureError",
        "CannotConnectNowError",
        "TooManyConnectionsError",
        "AdminShutdownError",
        "CrashShutdownError",
        "IdleSessionTimeoutError",
        "PostgresConnectionError",
        # Resource exhaustion (SQLSTATE 53xxx). Job e1192a9d died to a raw
        # "No space left on device" — an operator can expand the volume inside
        # the retry window, and if they don't, the ceiling fails the job anyway
        # with the infra cause named. asyncpg / psycopg spellings both listed.
        "DiskFullError",
        "DiskFull",
        "OutOfMemoryError",
        "OutOfMemory",
        "InsufficientResourcesError",
        # builtins raised through the socket layer
        "ConnectionResetError",
        "BrokenPipeError",
        "ConnectionAbortedError",
        "ConnectionRefusedError",
    }
)

# Overloaded classes — psycopg raises OperationalError for genuine config
# problems too, so these are gated on the message.
_MESSAGE_GATED_EXC_NAMES = frozenset({"OperationalError", "InterfaceError"})

_CONNECTION_LOST_MARKERS = (
    "the connection is closed",  # psycopg, the exact 2026-07-27 message
    "connection already closed",
    "connection is closed",
    "connection was closed",
    "server closed the connection unexpectedly",
    "terminating connection",
    "consuming input failed",
    "no connection to the server",
    "ssl connection has been closed",
    "connection reset",
    "connection refused",
    "could not connect",
    "server is not accepting connections",
    "the database system is in recovery mode",
    "the database system is starting up",
    "no space left on device",  # the error that killed job e1192a9d
)


def is_transient_infra_error(exc: BaseException) -> bool:
    """True when ``exc`` is a lost/unavailable backing service, not a job fault.

    A dropped Postgres connection terminally failed three multi-day jobs on
    2026-07-27 because the error taxonomy was binary: ``WorkspaceUnavailableError``
    was recoverable and *everything else* was death. This predicate adds the
    third class.

    Deliberately conservative — an unmatched exception keeps today's behaviour
    (terminal). The attempt ceiling on the retry path is the backstop for a
    misclassification in the other direction: a genuinely permanent error that
    slips in here fails the job after N attempts with the infra cause named,
    rather than retrying forever.
    """
    names = {cls.__name__ for cls in type(exc).__mro__}
    if names & _NEVER_TRANSIENT_EXC_NAMES:
        return False
    if names & _ALWAYS_TRANSIENT_EXC_NAMES:
        return True
    if names & _MESSAGE_GATED_EXC_NAMES:
        message = str(exc).lower()
        return any(marker in message for marker in _CONNECTION_LOST_MARKERS)
    return False


def completion_error_payload(exc: BaseException) -> Dict[str, Any]:
    """Build the completion-report ``error`` dict for an exception.

    The orchestrator's ``/complete`` recovery arms route purely on
    ``error.type`` — an untyped ``{"message": str(e)}`` report turns a
    recoverable failure into a terminal job failure plus workspace teardown.
    Every app-layer ``except`` that reports an exception to ``/complete`` must
    build its payload here so the classification survives.

    Four classes:
      * ``workspace_authentication`` — SSH credentials/config are invalid;
        terminal without workspace recovery.
      * ``workspace_unavailable`` — the VM/pod is gone; pause, reprovision,
        resume (knowledge-base/knowledge/issues/streaming_strips_workspace_unavailable_type.md).
      * ``infra_transient`` — a backing service blipped; pause, KEEP the
        workspace, retry with backoff
        (knowledge-base/knowledge/issues/transient_db_error_hard_fails_job_and_destroys_vm.md).
      * ``job_error`` — the job itself failed. Terminal.

    Order matters: deterministic authentication and dead-workspace errors are
    checked before the socket-level names in the transient set.
    """
    if isinstance(exc, WorkspaceAuthenticationError):
        error_type, recoverable = "workspace_authentication", False
    elif isinstance(exc, WorkspaceUnavailableError):
        error_type, recoverable = "workspace_unavailable", True
    elif is_transient_infra_error(exc):
        error_type, recoverable = "infra_transient", True
    else:
        error_type, recoverable = "job_error", False
    return {
        "error": {
            "message": str(exc),
            "type": error_type,
            "recoverable": recoverable,
        }
    }


class RemoteCommandTimeoutError(Exception):
    """A remote operation exceeded its wall-clock deadline.

    Deliberately NOT a WorkspaceUnavailableError: a slow command is not a
    dead workspace and must not trip the fast-freeze path — it surfaces to
    the model as an ordinary tool error instead. If the workspace is truly
    gone, the next operation's connect path classifies that and freezes.
    See knowledge-base/knowledge/issues/remote_backend_indefinite_wait_deadlock.md.
    """

    pass


class RemoteChannelBusyError(Exception):
    """sshd refused a session-channel open on a live transport.

    Deliberately NOT a WorkspaceUnavailableError: a per-connection session
    limit (OpenSSH MaxSessions) is a transient concurrency condition, not a
    dead workspace — misclassifying it triggers destructive pod recovery
    against a healthy pod. Surfaces to the model as an ordinary tool error.
    knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md.
    """

    pass


# Server-side cap on search_files matches. The display layer shows at most
# max_search_results (default 50); this bounds what backends ship over the
# wire. filesystem.py renders "N+ (capped)" when a result set hits this.
SEARCH_RESULT_HARD_CAP = 2000


def search_excludes(rel_to_search_root: str, exclude_dirs: Optional[List[str]]) -> bool:
    """True if a search hit lies inside one of ``exclude_dirs``.

    The in-process backends' half of ``grep -r --exclude-dir=GLOB`` (the SSH
    backend passes the flag straight to grep): every directory component below
    the search root is matched against each glob by name, so ``node_modules``
    skips a ``node_modules/`` at any depth. ``rel_to_search_root`` is the hit's
    path relative to the directory being searched; its last component is the
    file itself and never matches.
    """
    if not exclude_dirs:
        return False
    parts = PurePosixPath(rel_to_search_root).parts[:-1]
    return any(fnmatchcase(part, glob) for part in parts for glob in exclude_dirs)


class WorkspaceBackend(ABC):
    """Abstraction over workspace file storage and shell execution.

    All paths are relative to the workspace root. Implementations must
    validate that paths don't escape the workspace boundary.

    File operations (abstract): Every backend must implement these.
    Shell operations (non-abstract): Default to NotImplementedError.
    Override in backends that support shell execution (RemoteBackend).
    Backends without shell support get no shell tools — there is no local
    (in-pod) execution fallback (capability, not inference).
    """

    # --- File operations ---

    @abstractmethod
    def read_file(self, path: str, binary: bool = False) -> str | bytes:
        """Read file contents.

        Args:
            path: Relative path within workspace.
            binary: If True, return bytes instead of str.

        Returns:
            File contents as str (default) or bytes.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If path escapes workspace boundary.
        """
        ...

    @abstractmethod
    def write_file(self, path: str, content: str | bytes) -> None:
        """Write file contents. Creates parent directories.

        Args:
            path: Relative path within workspace.
            content: Content to write (str or bytes).

        Raises:
            ValueError: If path escapes workspace boundary.
        """
        ...

    def write_home_file(self, relative_path: str, content: str | bytes) -> None:
        """Write a file under the agent user's home directory ($HOME).

        Reserved for one-time setup that legitimately lives outside the
        workspace tree — e.g. SSH keys/config under ~/.ssh — without
        relaxing the boundary check that protects normal write_file calls.
        The path is interpreted relative to $HOME, so callers cannot write
        to arbitrary absolute paths.

        Default raises NotImplementedError; backends with a home concept
        (RemoteBackend) override.

        Args:
            relative_path: Path relative to $HOME (e.g. ".ssh/repo_foo").
            content: Content to write (str or bytes).

        Raises:
            ValueError: If path is empty, absolute, or escapes $HOME.
            NotImplementedError: If the backend has no home concept.
        """
        raise NotImplementedError("write_home_file not supported by this backend")

    def resolve_home_path(self, relative_path: str) -> str:
        """Resolve a path under $HOME to its canonical absolute form.

        Companion to write_home_file: lets callers obtain the absolute
        path they need to pass to shell commands (chmod, SSH config
        IdentityFile, etc.) without hard-coding the home directory.

        Default raises NotImplementedError; backends with a home concept
        override.

        Args:
            relative_path: Path relative to $HOME.

        Returns:
            Absolute path string.

        Raises:
            ValueError: If path is empty, absolute, or escapes $HOME.
            NotImplementedError: If the backend has no home concept.
        """
        raise NotImplementedError("resolve_home_path not supported by this backend")

    def install_credential_environment(self, values: Dict[str, str]) -> None:
        """Make connector ENV values available to this work item's commands."""
        raise ValueError("Credential connectors require a sandbox or VM workspace")

    def execute_with_secret_stdin(
        self, command: str, secret: str | bytes, *, timeout: int = 30
    ) -> bool:
        """Run trusted setup code while delivering a secret only on stdin.

        This is deliberately not part of the model-facing shell surface. It
        exists for control-plane bootstrap material that must never be placed
        in a command, environment variable, tmux scrollback, or workspace
        file. Backends without a private transport fail closed.
        """

        del command, secret, timeout
        raise NotImplementedError(
            "secret-stdin execution not supported by this backend"
        )

    @abstractmethod
    def append_file(self, path: str, content: str) -> None:
        """Append content to a file. Creates the file if it doesn't exist.

        Args:
            path: Relative path within workspace.
            content: Content to append.

        Raises:
            ValueError: If path escapes workspace boundary.
        """
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if path exists.

        Args:
            path: Relative path within workspace.

        Returns:
            True if the path exists (file or directory).
        """
        ...

    @abstractmethod
    def is_file(self, path: str) -> bool:
        """Check if path is a file.

        Args:
            path: Relative path within workspace.
        """
        ...

    @abstractmethod
    def is_dir(self, path: str) -> bool:
        """Check if path is a directory.

        Args:
            path: Relative path within workspace.
        """
        ...

    @abstractmethod
    def list_dir(self, path: str = "", pattern: str = "*") -> list[str]:
        """List directory contents matching a glob pattern.

        Args:
            path: Directory path relative to workspace root.
            pattern: Glob pattern to filter entries (default: "*").

        Returns:
            List of relative paths (from workspace root). Directories
            have a trailing "/".
        """
        ...

    @abstractmethod
    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> list[dict]:
        """Search for literal text in workspace files.

        One contract for every backend (the search_files tool promises it):
        ``query`` is a plain substring matched within single lines, never a
        regex or glob.

        Args:
            query: Literal text to search for.
            path: Directory or single file to search (default: entire workspace).
            case_sensitive: Whether search is case-sensitive.
            exclude_dirs: Directory-name globs to skip at any depth below
                ``path`` (see :func:`search_excludes`).

        Returns:
            List of dicts with 'path', 'line_number', and 'line'.
        """
        ...

    @abstractmethod
    def mkdir(self, path: str) -> None:
        """Create directory (and parents).

        Args:
            path: Relative path within workspace.

        Raises:
            ValueError: If path escapes workspace boundary.
        """
        ...

    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """Delete a file or empty directory.

        Args:
            path: Relative path within workspace.

        Returns:
            True if deleted, False if didn't exist.

        Raises:
            ValueError: If trying to delete non-empty directory.
        """
        ...

    @abstractmethod
    def delete_directory(self, path: str) -> bool:
        """Delete a directory and all its contents.

        Args:
            path: Relative path within workspace.

        Returns:
            True if deleted, False if didn't exist.

        Raises:
            ValueError: If path escapes workspace or is the workspace root.
        """
        ...

    @abstractmethod
    def move(self, src: str, dst: str) -> None:
        """Move a file or directory within the workspace.

        Args:
            src: Source path relative to workspace root.
            dst: Destination path relative to workspace root.

        Raises:
            FileNotFoundError: If source doesn't exist.
            ValueError: If paths escape workspace boundary.
        """
        ...

    def replace_file(self, src: str, dst: str) -> None:
        """Atomically replace one exact file target when the backend can.

        Unlike ``move``, an existing destination directory is never treated as
        a container. Cloud generation pulls use this narrower contract before
        acknowledging durable bytes; ordinary tool-facing move semantics stay
        unchanged for pinned and stateless sessions.
        """

        if self.is_dir(dst):
            raise ValueError(f"Destination is a directory: {dst}")
        self.move(src, dst)

    @abstractmethod
    def copy(self, src: str, dst: str) -> None:
        """Copy a file within the workspace.

        Args:
            src: Source path relative to workspace root.
            dst: Destination path relative to workspace root.

        Raises:
            FileNotFoundError: If source doesn't exist.
            ValueError: If source is a directory or paths escape boundary.
        """
        ...

    @abstractmethod
    def stat(self, path: str) -> int:
        """Get size of a file or directory in bytes.

        Args:
            path: Relative path within workspace (empty string = root).

        Returns:
            Size in bytes. 0 if path doesn't exist.
        """
        ...

    @abstractmethod
    def resolve_path(self, relative_path: str) -> str:
        """Resolve a relative path to its canonical form and validate boundaries.

        This is the equivalent of the old WorkspaceManager.get_path() — it
        validates that the path stays within the workspace root.

        Args:
            relative_path: Path relative to workspace root. Empty string = root.

        Returns:
            The resolved absolute path as a string.

        Raises:
            ValueError: If path escapes workspace boundary.
        """
        ...

    def walk(self, path: str = "") -> list[str]:
        """Recursively list every file path under ``path``, relative to the
        workspace root.

        Default implementation descends via ``list_dir`` (which returns
        root-relative paths, directories suffixed ``/``). Backends over a flat
        object store (``VirtualWorkspaceBackend``) override this with a single
        recursive listing — both for efficiency and because their ``list_dir``
        collapses to one level. Used by the cross-backend seed copy on a
        workspace-tier upgrade (``workspace_tier_upgrade.md`` §4.2 S3a).

        Args:
            path: Subtree to walk, relative to root (default: whole workspace).

        Returns:
            Sorted list of file paths (relative to root, no trailing slash).
            Directories are descended into but not themselves returned.
        """
        files: list[str] = []
        stack: list[str] = [path]
        seen: set[str] = set()
        while stack:
            current = stack.pop()
            for entry in self.list_dir(current):
                if entry in seen:
                    continue
                seen.add(entry)
                if entry.endswith("/"):
                    stack.append(entry.rstrip("/"))
                else:
                    files.append(entry)
        return sorted(files)

    # --- Properties ---

    @property
    def host(self) -> Optional[str]:
        """Remote host address, or None for local backends."""
        return None

    # --- Command execution ---

    def exec_command(self, command: str, timeout: int = 30) -> str:
        """Execute a command on the workspace host and return stdout.

        For remote backends, runs the command via SSH.

        Args:
            command: Shell command to execute.
            timeout: Timeout in seconds.

        Returns:
            Command stdout as string.
        """
        raise NotImplementedError("exec_command not supported by this backend")

    @property
    def claim_resource_fenced(self) -> bool:
        """Whether workspace-resident resource mutations have claim fencing.

        Generic and pinned backends retain their established unfenced
        behaviour.  A stateless ``RemoteBackend`` overrides this once it has
        been bound to a queue lease token.
        """

        return False

    def exec_claim_resource(
        self,
        command: str,
        timeout: int = 30,
        *,
        operation: str = "workspace resource mutation",
    ) -> str:
        """Execute one claim-scoped workspace resource operation.

        The default intentionally preserves pinned/custom-backend behaviour.
        Remote stateless workspaces override this with the cooperative durable
        token fence used by their resident tmux resource.  This primitive is
        separate from shell admission: retiring local shell tools must not
        prevent terminal mount cleanup while the claim still owns the lease.
        """

        del operation
        return self.exec_command(command, timeout=timeout)

    def retire_claim_resource_owner(self) -> None:
        """Close local admission for claim-scoped resource operations.

        Backends without a claim resource fence have nothing to retire.
        """

        return None

    # --- Shell operations ---
    #
    # Non-abstract: default to NotImplementedError. Override in backends
    # that support remote shell execution (RemoteBackend). ShellManager
    # refuses to construct without a shell-capable backend, so these
    # defaults are never reached through the tools.

    def shell_run(
        self,
        command: str,
        timeout: Optional[int] = None,
        tab_name: str = "default",
        working_dir: Optional[str] = None,
    ) -> str:
        """Execute a command synchronously with sentinel-based completion detection.

        Args:
            command: Shell command to execute.
            timeout: Hard timeout in seconds. None enables the soft no-change
                timeout (yields a "still running" result on quiet commands); an
                explicit value disables it and only the hard timeout applies.
            tab_name: Tab to execute in.
            working_dir: Working directory (relative to workspace root).

        Returns:
            Formatted output string identical to ShellManager.run_sync().
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_send(
        self,
        tab_name: str,
        text: str,
        enter: bool = True,
        working_dir: Optional[str] = None,
        allow_busy: bool = False,
    ) -> str:
        """Send keystrokes to a tab.

        Args:
            tab_name: Tab name.
            text: Text or tmux key names to send.
            enter: Whether to press Enter after sending.
            working_dir: Optional workspace-relative directory for a command.
                Backends must not apply it to raw keystrokes.
            allow_busy: Permit input to an already-running foreground process.
                This is for explicit keystroke/input mode, never a new async
                command.

        Returns:
            Confirmation message.
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_read(
        self,
        tab_name: str,
        lines: int = 50,
        since_cursor: bool = False,
    ) -> Tuple[str, Dict[str, Any]]:
        """Read output from a tab's terminal buffer.

        Args:
            tab_name: Tab name.
            lines: Number of lines to read from the end.
            since_cursor: If True, read only lines added since last read.

        Returns:
            Tuple of (text, metadata_dict).
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_read_with_offset(
        self,
        tab_name: str,
        lines: int = 30,
        offset: Optional[int] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Read output from a tab with optional absolute offset.

        Args:
            tab_name: Tab name.
            lines: Number of lines to return.
            offset: Absolute line position to start from (None = tail).

        Returns:
            Tuple of (text, metadata_dict).
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_ensure_tab(self, name: str) -> None:
        """Get or auto-create a tab.

        Args:
            name: Tab name (lowercase alphanumeric + hyphens, max 20 chars).
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_open_tab(
        self,
        name: str,
        command: Optional[str] = None,
        tab_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Open a new named tab.

        Args:
            name: Tab name.
            command: Optional command to run on creation.
            tab_type: Tab type (shell, ssh, repl, process).

        Returns:
            Metadata dict for the new tab.
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_close_tab(self, name: str) -> str:
        """Close a tab.

        Args:
            name: Tab name.

        Returns:
            Confirmation message.
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_cancel(self, tab_name: str = "default") -> str:
        """Send Ctrl+C to a tab to abort a stuck/hung command.

        Returns:
            Human-readable status of the cancel/reset.
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_list_tabs(self) -> List[Dict[str, Any]]:
        """Return metadata for all tabs."""
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_format_tab_header(self) -> str:
        """Return tab header string like [Shells: default | build]."""
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_cleanup(self) -> None:
        """Kill the entire shell session."""
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_reset_after_timeout(self) -> None:
        """Synchronously stop an owned timed-out shell before reconnecting.

        Unlike transport-only ``disconnect()``, this must prove that the exact
        owned shell was stopped before it returns. Implementations with durable
        ownership fences must preserve those fences while creating a clean
        shell generation for the same owner.
        """
        raise NotImplementedError("Shell operations not supported by this backend")

    def shell_is_alive(self) -> bool:
        """Check if the shell session exists."""
        raise NotImplementedError("Shell operations not supported by this backend")

    @property
    def supports_shell(self) -> bool:
        """Whether this backend supports shell operations.

        Returns True if shell_run() is implemented (not the default
        NotImplementedError). Gates ShellManager construction and shell
        tool registration — without it, shell tools are simply absent.
        """
        return False

    @property
    def supports_file_tools(self) -> bool:
        """Whether file tools (read_file/write_file/list_files/…) are exposed
        to the agent over this backend.

        True for real workspaces and the ``virtual`` tier (object-store file
        IO). False for the ``none`` tier's ScratchBackend, which exists only
        for internal consumers (PlanManager, todo archive) and registers no
        file tools (no_workspace_agent_mode.md §6). This gates *tool exposure*,
        not the backend's intrinsic ability to do file IO.
        """
        return True

    # Canvas is consumed by a different process than the agent. A backend must
    # therefore opt in only when the orchestrator can materialize the same
    # workspace generation through an attested transport. This is a mutable
    # class default so provisioned RemoteBackend instances can carry the
    # orchestrator's per-attach boolean without making generic/custom backends
    # accidentally presentable.
    supports_canvas_presentation: bool = False

    # Live workspace applications are a narrower capability than file
    # presentation. The orchestrator sets this only when the deployment-wide
    # live-preview gate is enabled *and* the exact remote workspace has the
    # attested Canvas transport required for loopback direct channels. Keeping
    # it separate prevents a file-capable backend from advertising ports.
    supports_canvas_live_apps: bool = False

    # Shared-browser presentation is separately attested because it also
    # requires an enabled broker and an orchestrator-reachable SSH target.
    # Generic, lite, custom, and unattested VM backends stay fail-closed.
    supports_canvas_shared_browser: bool = False

    # --- Lifecycle ---

    @abstractmethod
    def connect(self) -> None:
        """Establish connection (no-op for local)."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Clean up connection."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is connected and operational."""
        ...

    @property
    @abstractmethod
    def root(self) -> str:
        """Return the workspace root path as a string."""
        ...
