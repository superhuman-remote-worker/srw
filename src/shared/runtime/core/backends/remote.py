"""Remote workspace backend via SSH/SFTP.

Connects to a VM over SSH for file operations (SFTP) and shell execution
(remote tmux over SSH). Implements the same sentinel-based completion
detection as the local ShellManager, but over SSH channels.

Requires: paramiko (pip install paramiko)
See knowledge-base/knowledge/features/vm_backend.md for the full design.
"""

import base64
import errno
import fnmatch
import hashlib
import hmac
import logging
import os
import posixpath
import re
import shlex
import socket
import stat as stat_module
import threading
import time
import uuid
import weakref
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import paramiko
except ImportError:
    paramiko = None  # Deferred — only needed when backend: remote is used

from shared.runtime.core.shell_protocol import (
    COLLIDING_COMMAND_TEMPLATE,
    HARD_TIMEOUT_CAP_SECONDS,
    INTERACTIVE_PROMPT_PATTERNS,
    INTERACTIVE_PROMPT_TEMPLATE,
    NO_CHANGE_TIMEOUT_SECONDS,
    NONINTERACTIVE_ENV_EXPORT,
    STILL_RUNNING_HARDCAP_TEMPLATE,
    STILL_RUNNING_TEMPLATE,
    SUDO_FREEZE_SENTINEL,
    build_sentinel_command,
    compute_no_change_state,
)
from shared.runtime.core.workspace_backend import (
    SEARCH_RESULT_HARD_CAP,
    RemoteChannelBusyError,
    RemoteCommandTimeoutError,
    WorkspaceAuthenticationError,
    WorkspaceBackend,
    WorkspaceUnavailableError,
)

logger = logging.getLogger(__name__)

# Stall/timeout constants, interactive-prompt patterns and message templates
# are shared from shell_protocol (imported above) to keep the local and remote
# shell backends in lock-step.

# Connect-failure buckets → how many attempts each is worth.
_AMBIGUOUS_RETRY_CAP = 2
_GONE_ERRNOS = {errno.EHOSTUNREACH, errno.ENETUNREACH, errno.ENETDOWN}
_AUTH_ERROR_MARKERS = (
    "authentication failed",
    "authentication methods available",
    "not a valid private key",
    "private key file is encrypted",
    "permission denied (publickey",
)

# _exec output cap: past this, output is dropped (marker appended) but the
# channel keeps draining so the remote command can finish. Guards agent RAM.
_EXEC_MAX_OUTPUT_BYTES = 5 * 1024 * 1024
_EXEC_POLL_SECONDS = 0.05

# Session-channel opens refused by sshd on a LIVE transport (MaxSessions) are
# transient concurrency refusals, not workspace death: retry briefly, then
# surface as RemoteChannelBusyError (an ordinary tool error).
# knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md
_CHANNEL_OPEN_RETRIES = 3
_CHANNEL_OPEN_BACKOFF_SECONDS = 0.25

# shell_cancel: seconds to wait after a Ctrl+C before re-checking whether the
# tab returned to a prompt. Two attempts, then fall back to a tab reset.
_CANCEL_SETTLE_SECONDS = 0.5

# Transport-level keepalive + SFTP channel timeout: without these, a
# blackholed connection (network partition, silently-dead VM) hangs SFTP
# reads/writes forever while holding _sftp_lock, wedging ALL file ops.
_TRANSPORT_KEEPALIVE_SECONDS = 15
_SFTP_OP_TIMEOUT_SECONDS = 60.0
_TCP_KEEPALIVE_IDLE_SECONDS = 10
_TCP_KEEPALIVE_COUNT = 3
_TCP_KEEPALIVE_INTERVAL_SECONDS = 5
_TCP_USER_TIMEOUT_MILLIS = 10_000

# Explicit deadlines for heavy/recursive remote ops. Now that _exec's
# deadline actually binds (drain-loop fix), these need generous budgets or
# large trees would newly fail at the 30s default.
_HEAVY_OP_TIMEOUT_SECONDS = 300  # rm -rf, cp -a: recursive tree operations
_MEDIUM_OP_TIMEOUT_SECONDS = 120  # mv, du -sb: single-pass but can be slow


def _classify_connect_error(e: Exception) -> str:
    """Bucket an SSH connect failure to size the retry budget.

    'authentication' → the configured identity was rejected; retrying/recreating
                  the workspace with the same configuration cannot repair it.
    'gone'      → the workspace host is destroyed (DNS won't resolve / no route);
                  retrying is pointless, fail fast.
    'booting'   → host is up but sshd is not listening yet (ECONNREFUSED);
                  this is the boot window the retries exist for.
    'timeout'   → SYN/auth path timed out. For containers this stays a short
                  ambiguous retry; for VM backends it can mean the fresh
                  tailnet peer path is still converging.
    'ambiguous' → protocol / unknown; retry briefly then give up.
    """
    if paramiko is not None and isinstance(
        e, (paramiko.AuthenticationException, paramiko.PasswordRequiredException)
    ):
        return "authentication"
    if any(marker in str(e).lower() for marker in _AUTH_ERROR_MARKERS):
        return "authentication"
    if isinstance(e, socket.gaierror):
        return "gone"
    if isinstance(e, socket.timeout):
        return "timeout"
    if isinstance(e, ConnectionRefusedError):
        return "booting"
    if isinstance(e, OSError) and e.errno in _GONE_ERRNOS:
        return "gone"
    return "ambiguous"


def _validate_private_key(key_path: Optional[str]) -> str:
    """Validate the configured key as this worker UID; return public fingerprint."""
    if not key_path:
        raise WorkspaceAuthenticationError(
            "workspace.remote.key_path is missing; managed workspaces require "
            "an explicit private key"
        )
    if not os.path.isfile(key_path):
        raise WorkspaceAuthenticationError(
            f"workspace.remote.key_path does not exist: {key_path}"
        )
    try:
        with open(key_path, "rb"):
            pass
    except OSError as exc:
        raise WorkspaceAuthenticationError(
            f"workspace.remote.key_path is not readable by the worker: {key_path}"
        ) from exc
    try:
        private_key = paramiko.PKey.from_path(key_path)
    except (paramiko.SSHException, OSError, ValueError) as exc:
        raise WorkspaceAuthenticationError(
            "workspace.remote.key_path is invalid or passphrase-protected"
        ) from exc

    return _sha256_public_key_fingerprint(private_key)


def _sha256_public_key_fingerprint(key: Any) -> str:
    """Return the OpenSSH SHA256 fingerprint for a Paramiko public key."""

    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


def _validated_expected_host_key_fingerprint(value: Optional[str]) -> Optional[str]:
    """Validate an orchestrator-pinned OpenSSH SHA256 host identity."""

    if value is None:
        return None
    if (
        not isinstance(value, str)
        or re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", value) is None
    ):
        raise WorkspaceAuthenticationError(
            "Expected workspace SSH host key fingerprint must be exactly an "
            "OpenSSH SHA256 fingerprint"
        )
    return value


class WorkspaceHostIdentityMismatch(WorkspaceAuthenticationError):
    """The endpoint presented a host key other than its exact attestation.

    This remains an authentication failure on an initial attach: accepting a
    different key would cross the credential boundary.  On a previously live
    backend, however, the same signal means stable workspace DNS has moved to a
    replacement pod.  ``_ensure_connected`` translates that reconnect-only
    case to ``WorkspaceUnavailableError`` so the claim is handed off for a
    fresh orchestrator attestation instead of being terminally classified as a
    bad user credential.
    """


class _ExactSHA256HostKeyPolicy:
    """Paramiko host-key policy which accepts one exact SHA256 identity."""

    def __init__(self, expected_fingerprint: str):
        self._expected_fingerprint = expected_fingerprint

    def missing_host_key(self, client: Any, hostname: str, key: Any) -> None:
        observed = _sha256_public_key_fingerprint(key)
        if hmac.compare_digest(observed, self._expected_fingerprint):
            return
        try:
            client.close()
        finally:
            raise WorkspaceHostIdentityMismatch(
                "Workspace SSH host key fingerprint mismatch for "
                f"{hostname}: expected {self._expected_fingerprint}, got {observed}"
            )


# Tab name validation
TAB_NAME_PATTERN = re.compile(r"^[a-z0-9-]{1,20}$")

# Remote tmux owns the cross-agent shell continuity contract. User options are
# stored on each window so a fresh claimant can reconstruct the tool-facing tab
# model without an agent-local sidecar or PVC. Every field has a strict
# vocabulary/regex that excludes ``|``, which stays literal across the tmux
# 3.4 and 3.7 format engines (3.4 escapes ASCII control separators as ``\037``).
_TMUX_TAB_TYPE_OPTION = "@srw_tab_type"
_TMUX_PENDING_SENTINEL_OPTION = "@srw_pending_sentinel"
_TMUX_PENDING_SINCE_OPTION = "@srw_pending_since"
_TMUX_PANE_ID_OPTION = "@srw_pane_id"
_TMUX_OWNER_ID_OPTION = "@srw_owner_id"
_TMUX_OWNER_TOKEN_OPTION = "@srw_owner_token"
_TMUX_GENERATION_OPTION = "@srw_generation"
_TMUX_WORKSPACE_GENERATION_OPTION = "@srw_workspace_generation"
_TMUX_RUNTIME_INCARNATION_OPTION = "@srw_runtime_incarnation"
_TMUX_PROCESS_TAG_OPTION = "@srw_process_tag"
_TMUX_PROTOCOL_OPTION = "@srw_shell_protocol"
_TMUX_PROTOCOL_VERSION = "3"
_TMUX_SETUP_OPTION = "@srw_setup_state"
_TMUX_SETUP_PENDING = "pending"
_TMUX_SETUP_COMPLETE = "complete"
_TMUX_PROMPT_TOKEN_OPTION = "@srw_prompt_token"
_TMUX_WINDOW_PROMPT_OPTION = "@srw_window_prompt_token"
_TMUX_WINDOW_SETUP_OPTION = "@srw_window_setup_state"
_TMUX_WINDOW_STATE_OPTIONS = (
    _TMUX_TAB_TYPE_OPTION,
    _TMUX_PENDING_SENTINEL_OPTION,
    _TMUX_PENDING_SINCE_OPTION,
    _TMUX_PANE_ID_OPTION,
    _TMUX_WINDOW_PROMPT_OPTION,
    _TMUX_WINDOW_SETUP_OPTION,
)
_TMUX_FIELD_SEPARATOR = "|"
_TMUX_TAB_TYPES = frozenset({"shell", "ssh", "repl", "process"})
_INHERITED_BUSY_SENTINEL = "__SRW_INHERITED_BUSY__"
# Every durable pane guard is written together with a remote-clock epoch stamp
# (`@srw_pending_since`). A reserve attempt that loses the guard CAS normally
# refuses with exit 74, but a guard older than this bound is treated as
# abandoned, cleared, and replaced. The bound is safe against legitimately
# long commands (git clone/fetch) because a *tracked* guard never reaches the
# CAS: its owner either observes the completion record and CAS-clears it, or
# holds it in the local tab registry so colliding calls refuse at pre-flight.
# Only a claimant whose registry lost the guard (transport reconnect) races
# the CAS against it, and a single synchronous wait is already capped at
# HARD_TIMEOUT_CAP_SECONDS (600 s), after which the guard survives only as
# registry state. 1.5x that cap keeps abandoned guards from bricking a pane
# forever without reaping a freshly reserved one.
_PENDING_GUARD_STALE_SECONDS = 15 * 60
# Marker printed by the reserve CAS when it clears a stale guard so the
# Python side can log the recovered value and its age.
_STALE_GUARD_CLEARED_MARKER = "__SRW_STALE_GUARD_CLEARED__"
_TMUX_PANE_ID_PATTERN = re.compile(r"^%(?:0|[1-9][0-9]*)$")
_TMUX_PENDING_PATTERN = re.compile(r"^__DONE_[0-9a-f]{12}__$")
_TMUX_PROMPT_TOKEN_PATTERN = re.compile(r"^[0-9a-f]{32}$")
_TMUX_STATE_VERSION = "1"
_TMUX_LEGACY_INCARNATION_STATE_VERSION = "2"
_TMUX_INCARNATION_STATE_VERSION = "3"
_WORKSPACE_PROCESS_TAG_ENV = "SRW_WORKSPACE_PROCESS_TAG"
_SHELL_PROCESS_TAG_ENV = "SRW_SHELL_PROCESS_TAG"

# Default blocked commands
DEFAULT_BLOCKED_COMMANDS = frozenset(
    [
        "reboot",
        "shutdown",
        "poweroff",
        "halt",
        "init",
    ]
)


class _RemoteTab:
    """Tracks per-tab state for a remote tmux session."""

    def __init__(
        self,
        name: str,
        tab_type: str = "shell",
        *,
        pane_id: Optional[str] = None,
    ):
        self.name = name
        self.tab_type = tab_type
        self.pane_id = pane_id
        self.read_cursor: int = 0
        # Sentinel of a still-running command holding this tab (colliding guard).
        self.pending_sentinel: Optional[str] = None
        self.prompt_marker_ready = False
        self.created_at: datetime = datetime.now(timezone.utc)
        self.last_activity: datetime = datetime.now(timezone.utc)

    def to_metadata(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.tab_type,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
        }


def _parse_shell_completion_record(
    line: str,
    sentinel: str,
) -> Optional[Tuple[int, str]]:
    """Parse one exact shell-completion record.

    The command text itself is visible in tmux and can wrap at the terminal
    width.  A wrapped ``printf '<sentinel> %s ...'`` fragment is therefore not
    evidence that the command finished.  Only the exact three-field record
    emitted by the shell releases the durable pane guard.
    """
    parts = line.strip().split(maxsplit=2)
    if len(parts) != 3 or parts[0] != sentinel:
        return None
    if not parts[1].isdigit():
        return None
    exit_code = int(parts[1])
    if exit_code > 255:
        return None
    cwd = parts[2]
    if not cwd.startswith("/"):
        return None
    return exit_code, cwd


@dataclass(frozen=True, slots=True)
class _RemoteTerminalShellCleanupCapability:
    """Immutable exact authority exposing only one terminal cleanup call."""

    host: str
    port: int
    username: str
    key_path: Optional[str]
    workspace_path: str
    job_id: str
    scrollback_limit: int
    default_timeout: int
    no_change_timeout: float
    max_tabs: int
    blocked_commands: tuple[str, ...]
    sandbox_cwd: str
    connect_timeout: int
    max_retries: int
    retry_timeouts_as_booting: bool
    sudo_action: str
    sudo_block_message: Optional[str]
    workspace_generation: str
    runtime_incarnation: str
    expected_host_key_fingerprint: Optional[str]
    workspace_owner_kind: str
    workspace_owner_id: str
    shell_owner_token: int

    def __call__(self) -> None:
        cleanup_backend = RemoteBackend(
            host=self.host,
            port=self.port,
            username=self.username,
            key_path=self.key_path,
            workspace_path=self.workspace_path,
            job_id=self.job_id,
            scrollback_limit=self.scrollback_limit,
            default_timeout=self.default_timeout,
            no_change_timeout=self.no_change_timeout,
            max_tabs=self.max_tabs,
            blocked_commands=list(self.blocked_commands),
            sandbox_cwd=self.sandbox_cwd,
            connect_timeout=self.connect_timeout,
            max_retries=self.max_retries,
            retry_timeouts_as_booting=self.retry_timeouts_as_booting,
            sudo_action=self.sudo_action,
            sudo_block_message=self.sudo_block_message,
            workspace_generation=self.workspace_generation,
            runtime_incarnation=self.runtime_incarnation,
            expected_host_key_fingerprint=self.expected_host_key_fingerprint,
            workspace_owner_kind=self.workspace_owner_kind,
            workspace_owner_id=self.workspace_owner_id,
        )
        cleanup_backend.set_shell_owner_token(self.shell_owner_token)
        cleanup_backend.retire_shell_owner()
        cleanup_backend.retire_claim_resource_owner()
        try:
            cleanup_backend.shell_cleanup()
        finally:
            cleanup_backend.retire()


class RemoteBackend(WorkspaceBackend):
    """Workspace on a remote host (sandbox container or VM), accessed via SSH/SFTP.

    File operations use SFTP. Shell operations manage a remote tmux session
    via SSH exec_command. Uses the same sentinel-based completion detection
    as the local ShellManager.
    """

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "agent-host",
        key_path: Optional[str] = None,
        workspace_path: str = "/home/agent-host/workspace",
        job_id: str = "",
        scrollback_limit: int = 5000,
        default_timeout: int = 120,
        no_change_timeout: float = NO_CHANGE_TIMEOUT_SECONDS,
        max_tabs: int = 15,
        blocked_commands: Optional[List[str]] = None,
        sandbox_cwd: Optional[str] = None,
        connect_timeout: int = 30,
        max_retries: int = 5,
        retry_timeouts_as_booting: bool = False,
        sudo_action: str = "freeze",
        sudo_block_message: Optional[str] = None,
        workspace_generation: Optional[str] = None,
        runtime_incarnation: Optional[str] = None,
        expected_host_key_fingerprint: Optional[str] = None,
        require_host_key_fingerprint: bool = False,
        workspace_owner_kind: Optional[str] = None,
        workspace_owner_id: Optional[str] = None,
        workspace_tier: Optional[str] = None,
    ):
        if paramiko is None:
            raise ImportError(
                "paramiko is required for RemoteBackend. "
                "Install it: pip install paramiko"
            )

        self._host = host
        self._port = port
        self._username = username
        self._key_path = key_path
        self._remote_root = workspace_path.rstrip("/")
        self._job_id = job_id
        self._credential_env_path: str | None = None
        self._scrollback_limit = scrollback_limit
        self._default_timeout = default_timeout
        self._no_change_timeout = no_change_timeout
        self._max_tabs = max_tabs
        self._connect_timeout = connect_timeout
        self._max_retries = max_retries
        self._retry_timeouts_as_booting = retry_timeouts_as_booting
        self._has_connected_once = False
        self._sudo_action = sudo_action
        # Optional custom message for sudo_action="block" — carries the
        # operator's denial reason after a denied vm_upgrade request, so the
        # agent gets a reasoned rejection instead of the generic block text.
        self._sudo_block_message = sudo_block_message
        self._workspace_generation: Optional[str] = None
        if workspace_generation is not None:
            try:
                self._workspace_generation = str(uuid.UUID(str(workspace_generation)))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("workspace generation must be a UUID") from exc
        self._runtime_incarnation: Optional[str] = None
        if runtime_incarnation is not None:
            try:
                self._runtime_incarnation = str(uuid.UUID(str(runtime_incarnation)))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("runtime incarnation must be a UUID") from exc
        if (self._workspace_generation is None) != (self._runtime_incarnation is None):
            raise ValueError(
                "workspace generation and runtime incarnation must be supplied together"
            )
        if (workspace_owner_kind is None) != (workspace_owner_id is None):
            raise ValueError("workspace owner kind and id must be supplied together")
        if workspace_owner_kind is not None and self._workspace_generation is None:
            raise ValueError(
                "workspace owner authority requires generation and runtime incarnation"
            )
        if workspace_owner_kind is not None:
            if workspace_owner_kind not in {"job", "session"}:
                raise ValueError("workspace owner kind must be 'job' or 'session'")
            try:
                canonical_owner_id = str(uuid.UUID(str(workspace_owner_id)))
            except (TypeError, ValueError, AttributeError) as exc:
                raise ValueError("workspace owner id must be a UUID") from exc
            if str(workspace_owner_id) != canonical_owner_id:
                raise ValueError("workspace owner id must be a canonical UUID")
            self._workspace_owner_kind = workspace_owner_kind
            self._workspace_owner_id = canonical_owner_id
        else:
            # Persistent-session callers predate the explicit owner fields;
            # their remote job id has always been the session owner id.
            self._workspace_owner_kind = "session"
            self._workspace_owner_id = self._job_id
        # Sharing is an identity fact, never a caller-selectable cleanup mode:
        # a worker whose tmux job id differs from the explicit workspace owner
        # must not run workspace-wide process retirement.
        self._shared_workspace = (
            workspace_owner_kind is not None
            and self._workspace_owner_id != self._job_id
        )
        self._expected_host_key_fingerprint = _validated_expected_host_key_fingerprint(
            expected_host_key_fingerprint
        )
        if require_host_key_fingerprint and self._expected_host_key_fingerprint is None:
            raise WorkspaceAuthenticationError(
                "This Kubernetes workspace requires an orchestrator-attested "
                "SSH host key fingerprint"
            )
        if workspace_tier is not None and workspace_tier not in {
            "sandbox",
            "vm",
            "remote",
        }:
            raise ValueError("workspace tier must be sandbox, vm, or remote")
        self._workspace_tier = workspace_tier

        if blocked_commands is None:
            self._blocked_commands = DEFAULT_BLOCKED_COMMANDS
        else:
            self._blocked_commands = frozenset(blocked_commands)

        # sandbox_cwd: if set, cd to this directory in each new tab
        self._sandbox_cwd = sandbox_cwd or workspace_path.rstrip("/")

        # SSH/SFTP handles
        self._ssh: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._sftp_lock = (
            threading.RLock()
        )  # Guards all SFTP operations (not thread-safe)
        self._connection_lock = threading.RLock()

        # Lazily-resolved $HOME on the remote (cached after first lookup).
        # Used by write_home_file / resolve_home_path for setup writes
        # outside the workspace tree (SSH key/config, etc.).
        self._home_dir: Optional[str] = None

        # Shell state
        self._session_name = f"agent_{job_id[:12]}" if job_id else "agent_remote"
        self._tmux_lock_path = f"/tmp/srw-tmux-{self._session_name}.lock"
        state_key = hashlib.sha256(
            (job_id or self._session_name).encode("utf-8")
        ).hexdigest()
        self._tmux_state_filename = f"{state_key}.state"
        # The tmux name is intentionally shorter than the durable owner id.
        # Colliding full ids must still serialize before the full-owner option
        # rejects one of them, so the shared lock is keyed by the tmux name.
        lock_key = hashlib.sha256(self._session_name.encode("utf-8")).hexdigest()
        self._tmux_durable_lock_filename = f"{lock_key}.lock"
        self._tmux_owner_digest = state_key
        self._tabs: OrderedDict[str, _RemoteTab] = OrderedDict()
        # A pane pruned while reattaching must be reported to the agent once,
        # before the usual ensure-tab path is allowed to recreate it.
        self._lost_tab_notices: set[str] = set()
        # Synchronous commands must serialize per pane, not per backend.  A
        # shared-tree child can wait on its own tmux tab while the parent uses
        # another tab (notably ``git`` during turn finalization).  A backend-
        # wide lock makes that wait freeze the parent's event loop and prevents
        # live child controls from being accepted.  Weak values avoid retaining
        # locks forever when a long-lived session churns through tab names.
        self._tab_sync_locks_guard = threading.Lock()
        self._tab_sync_locks: weakref.WeakValueDictionary[str, Any] = (
            weakref.WeakValueDictionary()
        )
        self._shell_init_lock = threading.Lock()
        # Serializes the final local admission check with each remote tmux I/O.
        # cleanup can therefore retire this backend's shell owner without a
        # stale worker thread passing the check and submitting afterward.
        self._shell_io_lock = threading.RLock()
        # Resident workspace daemons share the durable tmux token + flock, but
        # have separate local admission.  Session detach retires shell tools
        # first; terminal cleanup must still be able to unmount while this
        # claim owns the queue lease.  The workspace marker/lock are writable
        # by the workload user, so this is a cooperative correctness fence,
        # not a security boundary against hostile workspace code.
        self._claim_resource_io_lock = threading.RLock()
        self._claim_resource_retired = False
        self._shell_initialized = False
        self._shell_owner_token: Optional[int] = None
        self._shell_generation: Optional[str] = None
        self._shell_retired = False
        self._prompt_token: Optional[str] = None
        self._prompt_marker: Optional[str] = None
        self._shell_protocol_current = False
        self._retired = False

        # Bound concurrent short-lived exec channels so a wide parallel tool
        # batch can never trip the workspace sshd's per-connection MaxSessions
        # limit (excess execs queue for the next free slot). Long-lived
        # channels (persistent SFTP, shell tabs) are the headroom between this
        # cap and MaxSessions.
        # knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md
        self._channel_slots = threading.Semaphore(
            max(1, int(os.environ.get("WORKSPACE_SSH_MAX_CONCURRENT_CHANNELS", "10")))
        )

        # Reconnect hook: fired after a genuine reconnect in _ensure_connected
        # (NOT the initial connect()). Lets the agent re-assert files it wrote on
        # top of the pod's git clone (task_brief.md, bound skills) that a pod
        # tear-down + re-provision would have dropped. Kept generic — the backend
        # knows nothing about *what* gets re-seeded. See
        # knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md (Issue 4).
        self._on_reconnect: Optional[Callable[[], None]] = None
        self._reseeding = False  # re-entrancy guard: the hook writes via us

    @property
    def root(self) -> str:
        return self._remote_root

    @property
    def host(self) -> str | None:
        return self._host

    @property
    def supports_shell(self) -> bool:
        return True

    @property
    def managed_repository_runtime_authority(self) -> tuple[str, str] | None:
        """Return the server-attested backing/runtime tuple when available.

        The managed-repository receipt also records a local PID-namespace
        incarnation, so legacy pinned lanes remain safe.  Supplying this tuple
        strengthens stateless and current persistent attaches by making an
        authority-generation mismatch an explicit replacement boundary.
        """

        if self._workspace_generation is None or self._runtime_incarnation is None:
            return None
        return self._workspace_generation, self._runtime_incarnation

    def set_shell_owner_token(self, lease_token: Optional[int]) -> None:
        """Bind future tmux mutations to a monotonic queue lease token.

        Pinned sessions pass ``None`` and rely on their reciprocal binding.
        Stateless claims pass the DB-issued token; changing it invalidates the
        local tab cache so the next shell operation promotes and rehydrates
        under the workspace-side token fence.
        """
        normalized = None if lease_token is None else int(lease_token)
        if normalized is not None and normalized < 0:
            raise ValueError("shell owner lease token must be non-negative")
        with self._shell_io_lock:
            if normalized == self._shell_owner_token:
                return
            if self._shell_owner_token is not None:
                raise WorkspaceUnavailableError(
                    "A remote shell backend cannot be rebound to a different "
                    "stateless claim; attach a fresh backend instance"
                )
            if self._shell_retired:
                raise WorkspaceUnavailableError(
                    "Remote shell owner has already been retired"
                )
            self._shell_owner_token = normalized
            self._shell_initialized = False
            self._tabs.clear()
            self._prompt_token = None
            self._prompt_marker = None
            self._shell_generation = None
            self._shell_protocol_current = False

    def retire_shell_owner(self) -> None:
        """Stop shell I/O while retaining the claim-resource generation.

        Resident cleanup deliberately runs after shell admission closes.  Its
        independent token/generation fence still needs the exact established
        shell generation until :meth:`retire` closes the whole backend.
        """
        with self._shell_io_lock:
            self._shell_retired = True
            self._shell_initialized = False
            self._tabs.clear()
            self._shell_protocol_current = False

    def make_terminal_shell_cleanup_capability(self) -> Callable[[], None]:
        """Fork a cleanup-only owner before retiring a stateless backend.

        A completion command can be durably accepted before its terminal job
        disposition is known.  The reporting worker must then retire this
        backend completely so a cancelled SFTP/resource thread cannot mutate
        after acceptance, while still retaining the ability to enact a later
        terminal tmux disposition.  Reusing this backend cannot satisfy both:
        :meth:`retire` deliberately prevents reconnects.

        The returned immutable callable stores only exact authority and is not
        exposed to tools.  On invocation it creates a private backend whose
        ordinary shell and claim-resource admissions start retired; its sole
        operation is ``shell_cleanup``, whose durable token/generation/runtime
        fences explicitly permit cleanup after local shell retirement.
        """

        if self._shell_owner_token is None or not self.workspace_incarnation_fenced:
            raise WorkspaceUnavailableError(
                "Terminal shell cleanup capability requires an exact "
                "stateless runtime fence"
            )
        assert self._workspace_generation is not None
        assert self._runtime_incarnation is not None
        assert self._workspace_owner_kind is not None
        assert self._workspace_owner_id is not None
        return _RemoteTerminalShellCleanupCapability(
            host=self._host,
            port=self._port,
            username=self._username,
            key_path=self._key_path,
            workspace_path=self._remote_root,
            job_id=self._job_id,
            scrollback_limit=self._scrollback_limit,
            default_timeout=self._default_timeout,
            no_change_timeout=self._no_change_timeout,
            max_tabs=self._max_tabs,
            blocked_commands=tuple(self._blocked_commands),
            sandbox_cwd=self._sandbox_cwd,
            connect_timeout=self._connect_timeout,
            max_retries=self._max_retries,
            retry_timeouts_as_booting=self._retry_timeouts_as_booting,
            sudo_action=self._sudo_action,
            sudo_block_message=self._sudo_block_message,
            workspace_generation=self._workspace_generation,
            runtime_incarnation=self._runtime_incarnation,
            expected_host_key_fingerprint=self._expected_host_key_fingerprint,
            workspace_owner_kind=self._workspace_owner_kind,
            workspace_owner_id=self._workspace_owner_id,
            shell_owner_token=self._shell_owner_token,
        )

    def claim_shell_owner(self) -> None:
        """Eagerly promote this stateless claim before attach-time shell work."""
        if self._shell_owner_token is None:
            return
        self._ensure_connected()
        started = time.perf_counter()
        disposition = self._create_or_observe_tmux_session()
        if self.workspace_incarnation_fenced:
            # Attach-time repository/cloud setup uses the claim-resource fence
            # before ShellManager lazily calls _init_shell(). Load the exact
            # generation established by the promotion now so those mutations
            # inherit the same process identity as later tmux tool work.
            generation = self._read_tmux_session_option(_TMUX_GENERATION_OPTION)
            if not re.fullmatch(r"[0-9a-f]{32}", generation):
                raise WorkspaceUnavailableError(
                    "Remote tmux process generation is malformed"
                )
            self._shell_generation = generation
        logger.info(
            "remote shell ownership timing: session=%s disposition=%s total=%.3fs",
            self._session_name,
            disposition,
            time.perf_counter() - started,
        )

    @property
    def claim_resource_fenced(self) -> bool:
        """True when this backend carries an exact stateless lease token."""

        return self._shell_owner_token is not None

    @property
    def workspace_incarnation_fenced(self) -> bool:
        """True when shell state is bound to orchestrator workspace authority."""

        return (
            self._workspace_generation is not None
            and self._runtime_incarnation is not None
        )

    @property
    def shared_workspace(self) -> bool:
        """True when this tmux job operates inside another owner's workspace."""

        return self._shared_workspace

    def _workspace_process_tag(self) -> str:
        """Exact cooperative process identity inherited by workspace residents."""

        if (
            not self.workspace_incarnation_fenced
            or not self._workspace_owner_kind
            or not self._workspace_owner_id
        ):
            raise AssertionError("workspace process tag requires exact session runtime")
        assert self._runtime_incarnation is not None
        return (
            f"v1:{self._workspace_owner_kind}:{self._workspace_owner_id}:"
            f"{self._runtime_incarnation}"
        )

    def _shell_process_tag(self, generation: str) -> str:
        if not self.workspace_incarnation_fenced or not self._job_id:
            raise AssertionError("shell process tag requires exact session runtime")
        assert self._workspace_generation is not None
        assert self._runtime_incarnation is not None
        return (
            f"v1:{self._job_id}:{self._workspace_generation}:"
            f"{self._runtime_incarnation}:{generation}"
        )

    def _stateless_process_env_export(self) -> str:
        if self._shell_generation is None:
            raise WorkspaceUnavailableError(
                "Stateless shell process generation is unavailable"
            )
        return (
            self._workspace_process_env_export() + " "
            f"{_SHELL_PROCESS_TAG_ENV}="
            f"{shlex.quote(self._shell_process_tag(self._shell_generation))}"
        )

    def _workspace_process_env_export(self) -> str:
        """Tag every descendant of this exact physical session workspace."""

        return (
            f"export {_WORKSPACE_PROCESS_TAG_ENV}="
            f"{shlex.quote(self._workspace_process_tag())}"
        )

    def _stateless_process_env_export_for_loaded_generation(self) -> str:
        """Tag terminal resource commands with the remotely loaded generation."""

        if not self.workspace_incarnation_fenced or not self._job_id:
            raise AssertionError(
                "terminal process tags require exact runtime authority"
            )
        assert self._workspace_generation is not None
        assert self._runtime_incarnation is not None
        shell_prefix = shlex.quote(
            f"v1:{self._job_id}:{self._workspace_generation}:"
            f"{self._runtime_incarnation}:"
        )
        return (
            f"export {_WORKSPACE_PROCESS_TAG_ENV}="
            f"{shlex.quote(self._workspace_process_tag())} "
            f'{_SHELL_PROCESS_TAG_ENV}={shell_prefix}"$_srw_generation"'
        )

    def _stateless_tmux_process_tag_check_shell(
        self,
        target: str,
        *,
        indent: str = "",
    ) -> str:
        """Validate tmux option+environment against the loaded generation."""

        if not self.workspace_incarnation_fenced:
            raise AssertionError("tmux process tags require exact runtime authority")
        assert self._workspace_generation is not None
        assert self._runtime_incarnation is not None
        prefix = shlex.quote(
            f"v1:{self._job_id}:{self._workspace_generation}:"
            f"{self._runtime_incarnation}:"
        )
        workspace_env = shlex.quote(
            _WORKSPACE_PROCESS_TAG_ENV + "=" + self._workspace_process_tag()
        )
        lines = (
            f'_srw_expected_shell_tag={prefix}"$_srw_generation"\n'
            "_srw_tmux_process_tag=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_PROCESS_TAG_OPTION}}}')\n"
            '[ "$_srw_tmux_process_tag" = "$_srw_expected_shell_tag" ] '
            "|| exit 81\n"
            "_srw_tmux_workspace_env=$(tmux show-environment -t "
            f"{target} {_WORKSPACE_PROCESS_TAG_ENV} 2>/dev/null || true)\n"
            f'[ "$_srw_tmux_workspace_env" = {workspace_env} ] || exit 81\n'
            "_srw_tmux_shell_env=$(tmux show-environment -t "
            f"{target} {_SHELL_PROCESS_TAG_ENV} 2>/dev/null || true)\n"
            f'[ "$_srw_tmux_shell_env" = "{_SHELL_PROCESS_TAG_ENV}='
            '$_srw_expected_shell_tag" ] || exit 81\n'
        )
        if not indent:
            return lines
        return "".join(f"{indent}{line}\n" for line in lines.splitlines())

    def _stateless_process_zero_shell(
        self,
        *,
        env_name: str,
        tag_argument: str,
        terminate: bool,
    ) -> str:
        """Kill/verify exactly tagged descendants, excluding this SSH chain."""

        mode = "terminate" if terminate else "verify"
        # Python avoids spawning grep/tr children whose inherited environment
        # could match the very tag being scanned. It also handles /proc stat
        # names with spaces and excludes the complete verifier ancestor chain
        # (entrypoint/sshd/flock/sh), while still finding disowned siblings.
        # A refusal ends the enclosing script: a caller may append its own
        # proof after this scan, and that command's success must never be
        # reported in place of the scan's 85/86.
        return f"""
python3 - {shlex.quote(env_name)} {tag_argument} {mode} <<'__SRW_PROCESS_ZERO_PY__' || exit $?
import os
import signal
import sys
import time

tag = (sys.argv[1] + "=" + sys.argv[2]).encode()
terminate = sys.argv[3] == "terminate"

ancestors = set()
pid = os.getpid()
while pid >= 1 and pid not in ancestors:
    ancestors.add(pid)
    if pid == 1:
        break
    try:
        stat = open(f"/proc/{{pid}}/stat", "rb").read()
        pid = int(stat.rsplit(b") ", 1)[1].split(None, 2)[1])
    except (OSError, ValueError, IndexError):
        break

UNSETTLED = object()

def environment(name):
    # Every thread of a group shares one mm and so one environment. A
    # leader without an mm (a zombie, an exiting task, or a dead main
    # thread whose siblings still run) opens with ESRCH on Linux 6.16+ and
    # reads empty before it, so ESRCH alone never proves the group stopped:
    # read through the tasks, where any task holding the mm is exact.
    try:
        values = open(f"/proc/{{name}}/environ", "rb").read()
    except ProcessLookupError:
        values = b""
    if values:
        return values.split(b"\\0")
    for _ in range(5):
        listed = os.listdir(f"/proc/{{name}}/task")
        for tid in listed:
            try:
                values = open(f"/proc/{{name}}/task/{{tid}}/environ", "rb").read()
            except (FileNotFoundError, ProcessLookupError):
                continue
            if values:
                return values.split(b"\\0")
        # No listed task holds an mm with an environment. A thread created
        # meanwhile appears in a second listing; without one, no task of
        # the group carries a tag it can still act on.
        if set(os.listdir(f"/proc/{{name}}/task")) <= set(listed):
            return None
    return UNSETTLED

def matching():
    found = []
    ambiguous = False
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        candidate = int(name)
        if candidate in ancestors:
            continue
        try:
            values = environment(name)
        except FileNotFoundError:
            continue
        except PermissionError:
            try:
                status = open(f"/proc/{{name}}/status", "rt").read().splitlines()
                uid_line = next(line for line in status if line.startswith("Uid:"))
                process_uid = int(uid_line.split()[1])
            except FileNotFoundError:
                continue
            except (OSError, StopIteration, ValueError, IndexError):
                raise SystemExit(86)
            if process_uid == os.geteuid():
                ambiguous = True
            continue
        except OSError:
            raise SystemExit(86)
        if values is UNSETTLED:
            ambiguous = True
        elif values is not None and tag in values:
            found.append(candidate)
    return found, ambiguous

targets, ambiguous = matching()
if terminate:
    for candidate in targets:
        try:
            os.kill(candidate, signal.SIGTERM)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and matching()[0]:
        time.sleep(0.1)
    for candidate in matching()[0]:
        try:
            os.kill(candidate, signal.SIGKILL)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and matching()[0]:
        time.sleep(0.05)
remaining, final_ambiguous = matching()
if remaining:
    raise SystemExit(85)
if ambiguous or final_ambiguous:
    raise SystemExit(86)
__SRW_PROCESS_ZERO_PY__
"""

    def _stateless_workspace_process_zero_shell(self, *, terminate: bool) -> str:
        """Kill/verify all processes belonging to this dedicated workspace."""

        return self._stateless_process_zero_shell(
            env_name=_WORKSPACE_PROCESS_TAG_ENV,
            tag_argument=shlex.quote(self._workspace_process_tag()),
            terminate=terminate,
        )

    def _dedicated_workspace_uid_process_zero_shell(self, *, terminate: bool) -> str:
        """Kill/verify every process owned by the dedicated workspace user.

        Protected sandbox workspaces are process-dedicated and sudo is
        unavailable.  An environment tag is useful diagnostics, but it is not
        an authority boundary: ``env -u ... setsid`` can deliberately shed it.
        The kernel UID is the non-bypassable boundary for this workspace Pod.
        Exclude only the exact SSH cleanup ancestry which must remain alive to
        report the proof, loop until the UID set is stably empty, and fail
        closed if any same-UID process cannot be inspected.
        """

        mode = "terminate" if terminate else "verify"
        return f"""
python3 - {mode} <<'__SRW_WORKSPACE_UID_ZERO_PY__'
import os
import signal
import sys
import time

terminate = sys.argv[1] == "terminate"
workspace_uid = os.geteuid()

ancestors = set()
pid = os.getpid()
while pid >= 1 and pid not in ancestors:
    ancestors.add(pid)
    if pid == 1:
        break
    try:
        stat = open(f"/proc/{{pid}}/stat", "rb").read()
        pid = int(stat.rsplit(b") ", 1)[1].split(None, 2)[1])
    except (OSError, ValueError, IndexError):
        break

def process_identity(name):
    # procfs may expose a PID directory while its status file is transiently
    # empty/truncated during reap. Re-inspect the exact PID for a bounded
    # interval; persistent unreadability remains a fail-closed ambiguity.
    for attempt in range(5):
        try:
            status = open(f"/proc/{{name}}/status", "rt").read().splitlines()
            uid_line = next(line for line in status if line.startswith("Uid:"))
            real_uid = int(uid_line.split()[1])
            state_line = next(line for line in status if line.startswith("State:"))
            threads_line = next(
                line for line in status if line.startswith("Threads:")
            )
            return real_uid, state_line.split()[1], int(threads_line.split()[1])
        except FileNotFoundError:
            return None
        except (PermissionError, OSError, StopIteration, ValueError, IndexError):
            if not os.path.exists(f"/proc/{{name}}"):
                return None
            if attempt < 4:
                time.sleep(0.005)
                continue
            print(
                f"workspace process-zero could not inspect PID {{name}}",
                file=sys.stderr,
            )
            raise SystemExit(86)

def same_uid_processes():
    found = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        candidate = int(name)
        if candidate in ancestors:
            continue
        identity = process_identity(name)
        if identity is None:
            continue
        real_uid, state, threads = identity
        # A zombie whose thread group has otherwise exited has lost every
        # userspace execution capability and cannot mutate the workspace.
        # Its parent/reaper may need the exact cleanup SSH ancestry to stay
        # alive long enough to collect it, so it must not make the
        # process-zero proof self-deadlock.  ``Threads`` counts every thread
        # until it is released, so a ``Z`` leader with more than one is a
        # dead main thread whose siblings still execute as this UID.
        if real_uid == workspace_uid and not (state == "Z" and threads == 1):
            found.append(candidate)
    return found

if terminate:
    for sig, budget in ((signal.SIGTERM, 5.0), (signal.SIGKILL, 2.0)):
        for candidate in same_uid_processes():
            try:
                os.kill(candidate, sig)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            if not same_uid_processes():
                break
            time.sleep(0.05)

# Require two stable empty observations so a forking writer cannot cross the
# single instant at which its parent disappears.
for _ in range(2):
    residual = same_uid_processes()
    if residual:
        print(
            "workspace process-zero residual PIDs: "
            + ",".join(str(candidate) for candidate in residual),
            file=sys.stderr,
        )
        raise SystemExit(85)
    time.sleep(0.05)
__SRW_WORKSPACE_UID_ZERO_PY__
"""

    def _stateless_shell_process_zero_shell(self, *, terminate: bool) -> str:
        """Kill/verify only this tmux owner's exact shell generation."""

        if not self.workspace_incarnation_fenced or not self._job_id:
            raise AssertionError("shell process zero requires exact runtime authority")
        assert self._workspace_generation is not None
        assert self._runtime_incarnation is not None
        prefix = shlex.quote(
            f"v1:{self._job_id}:{self._workspace_generation}:"
            f"{self._runtime_incarnation}:"
        )
        return (
            f'_srw_process_zero_tag={prefix}"$_srw_generation"\n'
            + self._stateless_process_zero_shell(
                env_name=_SHELL_PROCESS_TAG_ENV,
                tag_argument='"$_srw_process_zero_tag"',
                terminate=terminate,
            )
        )

    def _stateless_terminal_process_zero_shell(self, *, terminate: bool) -> str:
        """Use child-shell scope for shared workspaces, workspace scope otherwise."""

        if self._shared_workspace:
            return self._stateless_shell_process_zero_shell(terminate=terminate)
        return self._stateless_workspace_process_zero_shell(terminate=terminate)

    def retire_claim_resource_owner(self) -> None:
        """Wait out admitted resource I/O, then reject later local callers."""

        with self._claim_resource_io_lock:
            self._claim_resource_retired = True

    def exec_claim_resource(
        self,
        command: str,
        timeout: int = 30,
        *,
        operation: str = "workspace resource mutation",
    ) -> str:
        """Run a resident-resource operation under the claim's durable fence.

        Pinned callers execute the original command unchanged.  Stateless
        callers serialize with tmux ownership promotion under the same
        workspace-resident flock and validate the marker, full owner,
        generation, and exact queue token before the command starts.  Unlike
        tmux tool I/O this deliberately ignores ``_shell_retired``: terminal
        mount teardown follows shell retirement during session cleanup.

        Both the marker and flock live in the workload user's writable home.
        This prevents stale cooperating agents from mutating after a handoff;
        it is not an enforced boundary against hostile workspace processes.
        """

        if self._shell_owner_token is None:
            return self.exec_command(command, timeout=timeout)
        with self._claim_resource_io_lock:
            if self._claim_resource_retired:
                raise WorkspaceUnavailableError(
                    "Workspace claim-resource owner has been retired from this backend"
                )
            process_tag = (
                self._stateless_process_env_export()
                if self.workspace_incarnation_fenced
                else ""
            )
            inner = (
                self._stateless_tmux_fence_shell()
                + (process_tag + "\n" if process_tag else "")
                + self._with_credential_environment(command)
            )
            output, exit_code = self._exec_with_status(
                self._tmux_lock_command(inner, shell="bash"),
                timeout=timeout,
                retain_tail=True,
            )
            if exit_code != 0:
                raise WorkspaceUnavailableError(
                    f"Claim-fenced {operation} failed with exit code {exit_code}"
                )
            return output

    def execute_claim_resource_with_secret_stdin(
        self,
        command: str,
        secret: str | bytes | bytearray,
        *,
        timeout: int = 30,
        operation: str = "workspace secret resource mutation",
    ) -> bool:
        """Run trusted secret stdin under the exact stateless claim fence.

        Managed repository setup cannot use tmux because private key bytes may
        never enter scrollback or argv. This is the secret-stdin counterpart to
        :meth:`exec_claim_resource`: it holds the same local admission lock and
        wraps the remote command in the same token/generation flock before the
        private channel receives any bytes.
        """

        del operation  # retained for call-site audit symmetry
        if self._shell_owner_token is None:
            return self.execute_with_secret_stdin(command, secret, timeout=timeout)
        with self._claim_resource_io_lock:
            if self._claim_resource_retired:
                raise WorkspaceUnavailableError(
                    "Workspace claim-resource owner has been retired from this backend"
                )
            process_tag = (
                self._stateless_process_env_export()
                if self.workspace_incarnation_fenced
                else ""
            )
            inner = (
                self._stateless_tmux_fence_shell()
                + (process_tag + "\n" if process_tag else "")
                + command
            )
            return self.execute_with_secret_stdin(
                self._tmux_lock_command(inner, shell="bash"),
                secret,
                timeout=timeout,
            )

    def exec_terminal_claim_resource(
        self,
        command: str,
        timeout: int = 120,
        *,
        operation: str = "terminal workspace resource cleanup",
    ) -> str:
        """Run terminal resident cleanup after the queue token was advanced.

        Public End owns token ``T = N + 1`` while the durable workspace record
        normally still carries the losing claimant's token ``N``.  Ordinary
        :meth:`exec_claim_resource` deliberately requires exact equality and
        therefore cannot be used at this boundary.  This terminal-only
        primitive instead takes the same durable flock, validates the exact
        thread/backing/runtime owner, monotonically adopts ``T`` in every
        *existing* record, and then runs the caller's strict cleanup command.

        It never creates a missing tmux session.  A missing marker is accepted
        only when the exact tmux name is also absent; an active marker with a
        missing tmux is promoted in place so resident daemons can still be
        drained.  A marker naming another runtime is the retired predecessor's
        record on a Resume-reused volume: like :meth:`shell_cleanup`'s stale
        branch it is replaced only at or below ``T`` and only while the exact
        tmux name is absent.  The record remains ``active`` at ``T`` on success
        (and on a command failure), allowing :meth:`shell_cleanup` to be the
        sole writer of the terminal ``retired`` acknowledgement afterward.
        """

        if self._shell_owner_token is None or not self.workspace_incarnation_fenced:
            raise WorkspaceUnavailableError(
                "Terminal resource cleanup requires an exact stateless runtime fence"
            )
        with self._claim_resource_io_lock:
            if self._claim_resource_retired:
                raise WorkspaceUnavailableError(
                    "Workspace claim-resource owner has been retired from this backend"
                )
            inner = (
                self._stateless_terminal_resource_adoption_shell()
                + self._stateless_process_env_export_for_loaded_generation()
                + "\n"
                + command
            )
            with self._shell_io_lock:
                output, exit_code = self._exec_with_status(
                    self._tmux_lock_command(inner, shell="bash"),
                    timeout=timeout,
                    retain_tail=True,
                )
            if exit_code != 0:
                raise WorkspaceUnavailableError(
                    f"Terminal-fenced {operation} failed with exit code {exit_code}"
                )
            return output

    def verify_terminal_claim_resources_retired(
        self,
        command: str,
        timeout: int = 60,
        *,
        operation: str = "post-shell terminal resource verification",
    ) -> str:
        """Run a read-only zero proof under the exact retired remote record."""

        if self._shell_owner_token is None or not self.workspace_incarnation_fenced:
            raise WorkspaceUnavailableError(
                "Retired resource verification requires an exact stateless fence"
            )
        with self._claim_resource_io_lock:
            if self._claim_resource_retired:
                raise WorkspaceUnavailableError(
                    "Workspace claim-resource owner has been retired from this backend"
                )
            inner = self._stateless_retired_resource_fence_shell() + command
            with self._shell_io_lock:
                self._close_sftp_before_process_zero_proof()
                output, exit_code = self._exec_with_status(
                    self._tmux_lock_command(inner, shell="bash"),
                    timeout=timeout,
                    retain_tail=True,
                )
            if exit_code != 0:
                raise WorkspaceUnavailableError(
                    f"Terminal-fenced {operation} failed with exit code {exit_code}"
                )
            return output

    @property
    def sudo_action(self) -> str:
        """How this backend handles ``sudo`` (``"freeze"`` | ``"allow"`` |
        ``"block"``).

        Doubles as the tier discriminator for the live upgrade path: a ``vm``
        backend is built with ``sudo_action="allow"`` (its guest owns the sudo
        gate), a ``sandbox`` keeps ``"freeze"`` (sudo → VM-escalation). The
        workspace-upgrade handler reads this to tell sandbox from vm, since both
        report ``supports_shell == True`` (workspace_tier_upgrade.md Phase 2 /
        Q8).
        """
        return self._sudo_action

    def exec_command(self, command: str, timeout: int = 30) -> str:
        """Execute a command via SSH and return stdout.

        Public wrapper around _exec for use by tools that need to run
        commands on the workspace host (e.g., starting Chromium for CDP).
        """
        return self._exec(self._with_credential_environment(command), timeout=timeout)

    def _with_credential_environment(self, command: str) -> str:
        path = getattr(self, "_credential_env_path", None)
        if not path:
            return command
        return f". {shlex.quote(path)}; {command}"

    def install_credential_environment(self, values: Dict[str, str]) -> None:
        """Retain credentials in this session and source them for new commands.

        Existing processes keep their environment. Removed field names remain
        in the workspace; this method intentionally does not promise scrubbing.
        """
        import hashlib
        import json

        from shared.credential_connectors import normalize_credential_env
        from shared.runtime.core.credential_env import INSTALL_CREDENTIAL_ENV

        values = normalize_credential_env(values)
        if not values:
            return
        self._init_shell()
        identity = hashlib.sha256(
            (self._job_id or self._session_name).encode("utf-8")
        ).hexdigest()
        path = self._resolve_home_path(f".srw-credentials/{identity}.sh")
        command = (
            f"python3 -c {shlex.quote(INSTALL_CREDENTIAL_ENV)} {shlex.quote(path)}"
        )
        if not self.execute_claim_resource_with_secret_stdin(
            command, json.dumps(values), timeout=30
        ):
            raise WorkspaceUnavailableError("Could not install workspace credentials")
        self._credential_env_path = path

    def open_forward_channel(self, dest_host: str = "127.0.0.1", dest_port: int = 8080):
        """Open a ``direct-tcpip`` channel to a loopback port on the workspace.

        Returns a paramiko ``Channel`` (socket-like) tunnelled over the existing
        authenticated SSH transport, for reaching a guest-loopback service that
        is not exposed on the pod/VM network — e.g. code-server for a live-VM
        IDE session (knowledge-base/knowledge/features/vm_snapshots_and_ide.md, "Live-VM IDE Access
        via the Agent"). The workspace sshd permits exactly this via
        ``AllowTcpForwarding local`` + ``PermitOpen 127.0.0.1:*``.

        The returned channel is a *blocking* socket; a caller on an event loop
        must drive its ``recv``/``send`` off-thread (e.g. ``asyncio.to_thread``).
        It is independent of the shell/SFTP state, so an IDE session and the
        job's own workspace I/O multiplex over one transport without interfering.

        Raises:
            WorkspaceUnavailableError: if the SSH transport is not connected.
        """
        self._ensure_connected()
        transport = self._ssh.get_transport() if self._ssh else None
        if transport is None or not transport.is_active():
            raise WorkspaceUnavailableError(
                f"No active SSH transport to {self._host}:{self._port} "
                "for IDE port-forward"
            )
        # src_addr is informational (the notional channel origin); dest_addr is
        # the guest-loopback service we forward to.
        return transport.open_channel(
            "direct-tcpip", (dest_host, dest_port), ("127.0.0.1", 0)
        )

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def connect(self) -> None:
        """Establish the transport, serialized against terminal retirement."""
        with self._connection_lock:
            if self._retired:
                raise WorkspaceUnavailableError(
                    "Remote workspace backend has been retired from this owner"
                )
            self._connect_impl()

    def _connect_impl(self) -> None:
        """Establish SSH connection and SFTP channel.

        Retries to tolerate the window between daemon registration (NATS) and
        SSHD readiness, but classifies the failure (``_classify_connect_error``)
        so a workspace that is *gone* (DNS won't resolve / no route) fails fast
        instead of burning the full boot-window budget.
        See knowledge-base/knowledge/issues/agent_fast_freeze_on_dead_workspace.md.
        """
        fingerprint = _validate_private_key(self._key_path)
        logger.info(
            "Workspace SSH private key validated for %s:%d (fingerprint=%s)",
            self._host,
            self._port,
            fingerprint,
        )
        connect_kwargs = {
            "hostname": self._host,
            "port": self._port,
            "username": self._username,
            "timeout": self._connect_timeout,
            "key_filename": self._key_path,
            "allow_agent": False,
            "look_for_keys": False,
        }

        backoff = 2.0
        attempt = 0
        while True:
            attempt += 1
            self._ssh = paramiko.SSHClient()
            if self._expected_host_key_fingerprint is None:
                # Direct/pinned/VM endpoints retain their historical behavior.
                self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            else:
                self._ssh.set_missing_host_key_policy(
                    _ExactSHA256HostKeyPolicy(self._expected_host_key_fingerprint)
                )
            try:
                self._ssh.connect(**connect_kwargs)
                break
            except WorkspaceAuthenticationError:
                # A host-key mismatch is deterministic and must never consume
                # the boot retry budget or leave a half-open transport usable.
                try:
                    self._ssh.close()
                except Exception:
                    pass
                self._ssh = None
                raise
            except (paramiko.SSHException, socket.error, OSError) as e:
                bucket = _classify_connect_error(e)
                if bucket == "authentication":
                    raise WorkspaceAuthenticationError(
                        f"Workspace SSH authentication failed for "
                        f"{self._host}:{self._port} with key {fingerprint}: {e}"
                    ) from e
                if bucket == "gone":
                    effective_max = 1
                elif (
                    bucket == "timeout"
                    and self._retry_timeouts_as_booting
                    and not self._has_connected_once
                ):
                    effective_max = self._max_retries
                elif bucket in ("timeout", "ambiguous"):
                    effective_max = min(self._max_retries, _AMBIGUOUS_RETRY_CAP)
                else:  # booting
                    effective_max = self._max_retries
                if attempt >= effective_max:
                    raise WorkspaceUnavailableError(
                        f"Failed to connect to workspace "
                        f"{self._host}:{self._port} after {attempt} attempt(s) "
                        f"[{bucket}]: {e}"
                    ) from e
                logger.warning(
                    "SSH connect attempt %d/%d to %s:%d failed [%s] (%s), "
                    "retrying in %.0fs",
                    attempt,
                    effective_max,
                    self._host,
                    self._port,
                    bucket,
                    e,
                    backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 15.0)

        self._sftp = self._ssh.open_sftp()
        transport = self._ssh.get_transport()
        if transport is not None:
            transport.set_keepalive(_TRANSPORT_KEEPALIVE_SECONDS)
            try:
                sock = transport.get_socket()
            except Exception:
                sock = None
            if sock is not None:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    # Keepalive may be unsupported on custom transport stacks.
                    logger.debug(
                        "SO_KEEPALIVE unavailable for transport socket", exc_info=True
                    )
                if hasattr(socket, "TCP_KEEPIDLE"):
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_TCP,
                            socket.TCP_KEEPIDLE,
                            _TCP_KEEPALIVE_IDLE_SECONDS,
                        )
                    except Exception:
                        logger.debug(
                            "TCP_KEEPIDLE unavailable for transport socket",
                            exc_info=True,
                        )
                if hasattr(socket, "TCP_KEEPINTVL"):
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_TCP,
                            socket.TCP_KEEPINTVL,
                            _TCP_KEEPALIVE_INTERVAL_SECONDS,
                        )
                    except Exception:
                        logger.debug(
                            "TCP_KEEPINTVL unavailable for transport socket",
                            exc_info=True,
                        )
                if hasattr(socket, "TCP_KEEPCNT"):
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_TCP,
                            socket.TCP_KEEPCNT,
                            _TCP_KEEPALIVE_COUNT,
                        )
                    except Exception:
                        logger.debug(
                            "TCP_KEEPCNT unavailable for transport socket",
                            exc_info=True,
                        )
                if hasattr(socket, "TCP_USER_TIMEOUT"):
                    try:
                        sock.setsockopt(
                            socket.IPPROTO_TCP,
                            socket.TCP_USER_TIMEOUT,
                            _TCP_USER_TIMEOUT_MILLIS,
                        )
                    except Exception:
                        logger.debug(
                            "TCP_USER_TIMEOUT unavailable for transport socket",
                            exc_info=True,
                        )
        sftp_chan = self._sftp.get_channel()
        if sftp_chan is not None:
            sftp_chan.settimeout(_SFTP_OP_TIMEOUT_SECONDS)
        self._has_connected_once = True
        logger.info(f"Connected to workspace {self._host}:{self._port}")

    def disconnect(self) -> None:
        """Close only this backend's SSH/SFTP transport.

        The tmux session belongs to the durable remote workspace, not to an
        agent process or queue claim. Call :meth:`shell_cleanup` explicitly,
        while still connected, for a genuine session end or backend retirement.
        """
        self._shell_initialized = False
        self._tabs.clear()
        self._prompt_token = None
        self._prompt_marker = None
        self._shell_generation = None
        self._shell_protocol_current = False

        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None

        if self._ssh:
            try:
                self._ssh.close()
            except Exception:
                pass
            self._ssh = None

        # A replacement workspace pod may resolve the same configured endpoint
        # to a different home. Never carry the old SFTP normalization across a
        # transport lifetime.
        self._home_dir = None

        logger.info(f"Disconnected from workspace {self._host}")

    def retire(self) -> None:
        """Permanently close this backend instance without killing remote tmux.

        Unlike a transport reset, session cleanup is terminal for the Python
        object. Worker-thread cancellation cannot stop a synchronous Paramiko
        call already in progress, so the retired bit prevents that stale call
        from reconnecting and mutating the successor's workspace afterward.
        """
        # Match exec_claim_resource's lock order (resource admission before
        # transport).  A fallback retirement that runs without manager cleanup
        # waits for any already-admitted mutation, rejects later ones, and only
        # then tears down SSH/SFTP.
        with self._claim_resource_io_lock:
            self._claim_resource_retired = True
            # File tools use the persistent SFTP channel rather than the tmux
            # flock.  Wait for any already-admitted read/write to leave its
            # channel before declaring this Python owner retired; otherwise a
            # terminal lifecycle ACK could race a late write into the snapshot
            # that follows. New callers fail at _ensure_connected once the bit
            # below is published (a caller that passed the pre-lock check can
            # only reach a closed channel after us, never mutate it).
            with self._sftp_lock:
                with self._connection_lock:
                    self._retired = True
                    self.disconnect()

    def is_connected(self) -> bool:
        if self._ssh is None:
            return False
        transport = self._ssh.get_transport()
        return transport is not None and transport.is_active()

    def set_reconnect_hook(self, hook: Optional[Callable[[], None]]) -> None:
        """Register a callback fired after a genuine reconnect (see
        ``_ensure_connected``). Used by the agent to idempotently re-assert
        files it wrote on top of the pod's git clone, which a pod tear-down +
        re-provision drops (task_brief.md, bound skills)."""
        self._on_reconnect = hook

    def _ensure_connected(self) -> None:
        """Reconnect if the SSH connection is dead.

        connect() owns the ENTIRE retry budget (attempts, backoff, cause
        classification). This method must NOT wrap it in a second retry loop —
        the nested loops multiplied the budget to max_retries² and turned a
        dead-workspace call into a ~15-min stall.
        See knowledge-base/knowledge/issues/agent_fast_freeze_on_dead_workspace.md.
        """
        if self._retired:
            raise WorkspaceUnavailableError(
                "Remote workspace backend has been retired from this owner"
            )
        if self.is_connected():
            return
        logger.warning(f"SSH connection to {self._host} lost, reconnecting...")
        shell_was_initialized = self._shell_initialized
        # The endpoint may now resolve to a replacement workspace pod. Any
        # value derived through the old SFTP transport is invalid even when the
        # configured host string is unchanged.
        self._home_dir = None
        try:
            self.connect()
        except WorkspaceHostIdentityMismatch as exc:
            # Stable Service DNS resolving to a different exact host key after
            # this backend was already live is a workspace-incarnation change,
            # not a suddenly-invalid private key.  This object only carries the
            # old orchestrator attestation, so it must fail as recoverable
            # workspace loss and let the stateless driver obtain a fresh bundle.
            raise WorkspaceUnavailableError(
                "Workspace SSH host identity changed during reconnect; "
                "fresh workspace attestation is required"
            ) from exc
        if shell_was_initialized and self._shell_owner_token is not None:
            # The next shell operation must re-check the remote tmux identity
            # and rebuild tab state. A same-workspace reconnect preserves tmux;
            # an active marker with a missing tmux server fails closed until a
            # controller-attested runtime-replacement path is implemented.
            self._shell_initialized = False
            self._tabs.clear()
        logger.info(f"Reconnected to {self._host}")
        # A reconnect means the SSH transport died — often because the workspace
        # pod was torn down and re-provisioned. The new pod has its git clone but
        # not the files the agent wrote on top, so re-assert them. Guard against
        # re-entrancy: the hook writes files through us, which re-enters
        # _ensure_connected (now live → no-op), but a drop mid-hook must not
        # recurse into the hook again.
        if self._on_reconnect is not None and not self._reseeding:
            self._reseeding = True
            try:
                self._on_reconnect()
            except Exception:
                logger.exception(
                    "Workspace re-seed hook failed after reconnect (non-fatal)"
                )
            finally:
                self._reseeding = False

    def _exec(self, command: str, timeout: int = 30) -> str:
        """Execute a command via SSH and return stdout.

        Drains stdout AND stderr while waiting for the exit status, so output
        larger than the SSH channel window cannot deadlock the command
        (knowledge-base/knowledge/issues/remote_backend_indefinite_wait_deadlock.md), and enforces
        ``timeout`` as a wall-clock deadline on the whole command.

        Raises WorkspaceUnavailableError on connection failure,
        RemoteCommandTimeoutError when the deadline expires, and
        RemoteChannelBusyError when sshd keeps refusing session channels on a
        live transport (MaxSessions saturation — NOT workspace death; see
        knowledge-base/knowledge/issues/maxsessions_parallel_tools_false_workspace_death.md).
        """
        output, _ = self._exec_with_status(command, timeout=timeout)
        return output

    def _exec_with_status(
        self,
        command: str,
        timeout: int = 30,
        *,
        retain_tail: bool = False,
    ) -> tuple[str, int]:
        """Execute via SSH and return bounded stdout plus remote exit status."""
        self._ensure_connected()
        last_refusal: Optional[paramiko.ChannelException] = None
        for attempt in range(_CHANNEL_OPEN_RETRIES + 1):
            if self._retired:
                raise WorkspaceUnavailableError(
                    "Remote workspace backend has been retired from this owner"
                )
            try:
                return self._exec_once(command, timeout, retain_tail=retain_tail)
            except paramiko.ChannelException as e:
                # A clean CHANNEL_OPEN_FAILURE arrives over a working
                # transport. Dead transport → genuine workspace loss; live
                # transport → per-connection session-limit refusal, retryable
                # in milliseconds once a running command finishes.
                transport = self._ssh.get_transport() if self._ssh else None
                if transport is None or not transport.is_active():
                    raise WorkspaceUnavailableError(
                        f"SSH command failed on {self._host}: {e}"
                    ) from e
                last_refusal = e
                if attempt < _CHANNEL_OPEN_RETRIES:
                    logger.warning(
                        f"sshd refused session channel on live transport to "
                        f"{self._host} (attempt {attempt + 1}/"
                        f"{_CHANNEL_OPEN_RETRIES + 1}) — retrying"
                    )
                    time.sleep(_CHANNEL_OPEN_BACKOFF_SECONDS * (2**attempt))
            except (paramiko.SSHException, socket.error, EOFError, OSError) as e:
                raise WorkspaceUnavailableError(
                    f"SSH command failed on {self._host}: {e}"
                ) from e
        raise RemoteChannelBusyError(
            f"sshd on {self._host} refused a session channel "
            f"{_CHANNEL_OPEN_RETRIES + 1} times while the transport stayed "
            f"active (too many concurrent SSH sessions). The workspace is "
            f"healthy — retry the command."
        ) from last_refusal

    def _exec_once(
        self, command: str, timeout: int, *, retain_tail: bool = False
    ) -> tuple[str, int]:
        """Single exec attempt — open channel, drain, return stdout + status.

        Raises paramiko/socket errors raw; ``_exec`` owns classification.
        Holds a ``_channel_slots`` permit for the channel's whole lifetime and
        closes the channel explicitly so the server-side session slot frees
        as soon as the command is done.
        """
        with self._channel_slots:
            ssh = self._ssh
            if ssh is None or self._retired:
                raise WorkspaceUnavailableError(
                    "Remote workspace transport retired before command start"
                )
            _, stdout, _ = ssh.exec_command(command, timeout=timeout)
            chan = stdout.channel
            try:
                return self._drain_exec_channel(
                    chan,
                    command,
                    timeout,
                    retain_tail=retain_tail,
                )
            finally:
                chan.close()

    def _drain_exec_channel(
        self,
        chan,
        command: str,
        timeout: int,
        *,
        retain_tail: bool = False,
        sensitive: bool = False,
    ) -> tuple[str, int]:
        """Drain stdout/stderr until exit under a wall-clock deadline."""
        out_chunks: list[bytes] = []
        err_chunks: list[bytes] = []
        out_size = 0
        err_size = 0
        truncated = False
        deadline = time.monotonic() + timeout
        while True:
            # Bound the inner drains by the deadline too: a producer
            # that keeps recv_ready() True (output arriving faster than
            # we can be pre-empted) would otherwise evade the timeout
            # forever, since the deadline check below only ran once
            # BOTH buffers went momentarily empty.
            while chan.recv_ready() and time.monotonic() <= deadline:
                chunk = chan.recv(65536)
                if sensitive:
                    pass
                elif retain_tail:
                    out_chunks.append(chunk)
                    retained = sum(len(part) for part in out_chunks)
                    while retained > _EXEC_MAX_OUTPUT_BYTES and out_chunks:
                        excess = retained - _EXEC_MAX_OUTPUT_BYTES
                        if excess >= len(out_chunks[0]):
                            retained -= len(out_chunks.pop(0))
                        else:
                            out_chunks[0] = out_chunks[0][excess:]
                            retained -= excess
                    if out_size + len(chunk) > _EXEC_MAX_OUTPUT_BYTES:
                        truncated = True
                elif out_size < _EXEC_MAX_OUTPUT_BYTES:
                    remaining = _EXEC_MAX_OUTPUT_BYTES - out_size
                    out_chunks.append(chunk[:remaining])
                    if len(chunk) > remaining:
                        truncated = True
                else:
                    truncated = True
                out_size += len(chunk)
            while chan.recv_stderr_ready() and time.monotonic() <= deadline:
                chunk = chan.recv_stderr(65536)
                if not sensitive and err_size < _EXEC_MAX_OUTPUT_BYTES:
                    err_chunks.append(chunk)
                err_size += len(chunk)
            if (
                chan.exit_status_ready()
                and not chan.recv_ready()
                and not chan.recv_stderr_ready()
            ):
                break
            if time.monotonic() > deadline:
                raise RemoteCommandTimeoutError(
                    f"Remote command timed out after {timeout}s on "
                    f"{self._host}: {command[:80]}"
                )
            time.sleep(_EXEC_POLL_SECONDS)
        exit_code = chan.recv_exit_status()  # ready — returns immediately
        output = (
            "" if sensitive else b"".join(out_chunks).decode("utf-8", errors="replace")
        )
        if truncated:
            notice = "[output truncated at 5 MiB]"
            output = f"{notice}\n{output}" if retain_tail else f"{output}\n{notice}"
        if exit_code != 0 and not sensitive:
            err = b"".join(err_chunks).decode("utf-8", errors="replace")
            # Some commands (grep with no match, tmux has-session) use non-zero
            # exit codes for normal conditions — callers check output.
            logger.debug(
                f"Remote command exit {exit_code}: {command[:80]} | stderr: {err[:200]}"
            )
        return output, exit_code

    # =========================================================================
    # Path utilities
    # =========================================================================

    def _resolve(self, relative_path: str) -> str:
        """Resolve relative path to absolute remote path, validate boundaries."""
        if not relative_path or relative_path == ".":
            return self._remote_root

        # Normalize: remove leading /, collapse .., etc.
        cleaned = posixpath.normpath(relative_path)

        # Security: reject paths that escape
        if cleaned.startswith("..") or cleaned.startswith("/"):
            raise ValueError(f"Path '{relative_path}' escapes workspace boundary")

        full = posixpath.join(self._remote_root, cleaned)

        # Double-check normalization didn't allow escape
        if not full.startswith(self._remote_root):
            raise ValueError(f"Path '{relative_path}' escapes workspace boundary")

        return full

    def resolve_path(self, relative_path: str) -> str:
        return self._resolve(relative_path)

    def _get_home_dir(self) -> str:
        """Lazily resolve and cache the agent user's $HOME via SFTP.

        SFTP's default cwd after connection is the user's home directory,
        so normalize(".") returns its canonical absolute path. Cached for
        the lifetime of the backend.
        """
        if self._home_dir is not None:
            return self._home_dir
        self._ensure_connected()
        with self._sftp_lock:
            self._home_dir = self._sftp.normalize(".")
        return self._home_dir

    def _resolve_home_path(self, relative_path: str) -> str:
        """Resolve a path under $HOME, rejecting empty/absolute/escaping inputs."""
        if not relative_path or relative_path == ".":
            raise ValueError("resolve_home_path requires a non-empty relative path")
        cleaned = posixpath.normpath(relative_path)
        if cleaned.startswith("..") or cleaned.startswith("/"):
            raise ValueError(f"Path '{relative_path}' escapes home directory")
        home = self._get_home_dir()
        full = posixpath.join(home, cleaned)
        if not full.startswith(home + "/") and full != home:
            raise ValueError(f"Path '{relative_path}' escapes home directory")
        return full

    def _remote_stat(self, remote_path: str) -> Optional[paramiko.SFTPAttributes]:
        """Get SFTP stat, returning None if path doesn't exist.

        socket.timeout is an IOError subclass, so it must be special-cased
        BEFORE the generic IOError handler — otherwise a stalled channel
        (see _SFTP_OP_TIMEOUT_SECONDS) reads as "path doesn't exist",
        which corrupts exists/is_file/is_dir/move/copy/stat/delete_directory
        semantics all built on top of this method.
        """
        self._ensure_connected()
        with self._sftp_lock:
            try:
                return self._sftp.stat(remote_path)
            except FileNotFoundError:
                return None
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out stat {remote_path}"
                ) from e
            except IOError:
                return None

    def _ensure_remote_dir(self, remote_path: str) -> None:
        """Recursively create directories on the remote."""
        # Walk up to find an existing ancestor, then create downward
        parts_to_create = []
        current = remote_path
        while current and current != "/":
            st = self._remote_stat(current)
            if st is not None:
                break
            parts_to_create.append(current)
            current = posixpath.dirname(current)

        with self._sftp_lock:
            for d in reversed(parts_to_create):
                try:
                    self._sftp.mkdir(d)
                except socket.timeout as e:
                    raise RemoteCommandTimeoutError(
                        f"Workspace I/O timed out mkdir {d}"
                    ) from e
                except IOError:
                    # Race condition or already exists
                    pass

    # =========================================================================
    # File operations
    # =========================================================================

    def read_file(self, path: str, binary: bool = False) -> str | bytes:
        self._ensure_connected()
        remote_path = self._resolve(path)
        with self._sftp_lock:
            try:
                with self._sftp.open(remote_path, "rb") as f:
                    data = f.read()
            except FileNotFoundError:
                raise FileNotFoundError(f"File not found: {path}")
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out reading {path}"
                ) from e
            except IOError as e:
                raise FileNotFoundError(f"Cannot read {path}: {e}") from e

        if binary:
            return data
        return data.decode("utf-8")

    def write_file(self, path: str, content: str | bytes) -> None:
        self._ensure_connected()
        remote_path = self._resolve(path)
        parent = posixpath.dirname(remote_path)
        self._ensure_remote_dir(parent)

        data = content if isinstance(content, bytes) else content.encode("utf-8")
        with self._sftp_lock:
            try:
                with self._sftp.open(remote_path, "wb") as f:
                    f.write(data)
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out writing {path}"
                ) from e
        logger.debug(f"Wrote remote file: {path}")

    def write_home_file(self, relative_path: str, content: str | bytes) -> None:
        self._ensure_connected()
        remote_path = self._resolve_home_path(relative_path)
        parent = posixpath.dirname(remote_path)
        self._ensure_remote_dir(parent)

        data = content if isinstance(content, bytes) else content.encode("utf-8")
        with self._sftp_lock:
            try:
                with self._sftp.open(remote_path, "wb") as f:
                    f.write(data)
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out writing {relative_path}"
                ) from e
        logger.debug(f"Wrote remote home file: {relative_path}")

    def resolve_home_path(self, relative_path: str) -> str:
        return self._resolve_home_path(relative_path)

    def execute_with_secret_stdin(
        self, command: str, secret: str | bytes | bytearray, *, timeout: int = 30
    ) -> bool:
        """Run trusted bootstrap code with secret bytes sent over SSH stdin.

        Unlike ``shell_run`` this bypasses tmux entirely, so neither command
        history nor scrollback can contain the input. Output is drained and
        discarded. The caller must supply only a server-authored command and
        must never interpolate the secret into it.
        """

        self._ensure_connected()
        if isinstance(secret, bytearray):
            payload = secret
        elif isinstance(secret, bytes):
            payload = bytearray(secret)
        else:
            payload = bytearray(secret.encode("utf-8"))
        stdout = None
        with self._channel_slots:
            ssh = self._ssh
            if ssh is None or self._retired:
                raise WorkspaceUnavailableError(
                    "Remote workspace transport retired before secret bootstrap"
                )
            try:
                stdin, stdout, _stderr = ssh.exec_command(command, timeout=timeout)
                stdin.write(payload)
                stdin.flush()
                stdin.channel.shutdown_write()
                payload[:] = b"\x00" * len(payload)
                _output, returncode = self._drain_exec_channel(
                    stdout.channel,
                    command,
                    timeout,
                    retain_tail=False,
                    sensitive=True,
                )
                return returncode == 0
            except (paramiko.SSHException, socket.error, EOFError, OSError) as exc:
                raise WorkspaceUnavailableError(
                    "Remote secret bootstrap transport failed"
                ) from exc
            finally:
                payload[:] = b"\x00" * len(payload)
                if stdout is not None:
                    try:
                        stdout.channel.close()
                    except (AttributeError, OSError):
                        pass

    def append_file(self, path: str, content: str) -> None:
        self._ensure_connected()
        remote_path = self._resolve(path)
        parent = posixpath.dirname(remote_path)
        self._ensure_remote_dir(parent)

        with self._sftp_lock:
            try:
                with self._sftp.open(remote_path, "ab") as f:
                    f.write(content.encode("utf-8"))
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out writing {path}"
                ) from e

    def exists(self, path: str) -> bool:
        return self._remote_stat(self._resolve(path)) is not None

    def is_file(self, path: str) -> bool:
        st = self._remote_stat(self._resolve(path))
        if st is None:
            return False
        return stat_module.S_ISREG(st.st_mode)

    def is_dir(self, path: str) -> bool:
        st = self._remote_stat(self._resolve(path))
        if st is None:
            return False
        return stat_module.S_ISDIR(st.st_mode)

    def list_dir(self, path: str = "", pattern: str = "*") -> list[str]:
        self._ensure_connected()
        remote_path = self._resolve(path)

        st = self._remote_stat(remote_path)
        if st is None:
            return []
        if not stat_module.S_ISDIR(st.st_mode):
            return [path]

        with self._sftp_lock:
            try:
                entries = self._sftp.listdir_attr(remote_path)
            except socket.timeout as e:
                raise RemoteCommandTimeoutError(
                    f"Workspace I/O timed out listing {remote_path}"
                ) from e
            except IOError:
                return []

        results = []
        for entry in entries:
            entry_remote = posixpath.join(remote_path, entry.filename)
            # Compute path relative to workspace root
            if entry_remote.startswith(self._remote_root + "/"):
                rel = entry_remote[len(self._remote_root) + 1 :]
            else:
                rel = entry.filename

            # Apply glob pattern
            if not fnmatch.fnmatch(entry.filename, pattern):
                continue

            if stat_module.S_ISDIR(entry.st_mode):
                results.append(rel + "/")
            else:
                results.append(rel)

        return sorted(results)

    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> list[dict]:
        """Search via server-side grep for efficiency."""
        self._ensure_connected()
        remote_path = self._resolve(path)

        # Build grep command. -F: the query is literal text, as on every other
        # backend — without it grep read a basic regex, so ``x[0]`` never found
        # ``x[0]`` and ``a.c`` also hit ``abc``. -H: GNU grep prints no filename
        # for a single-file ``path`` even with -r, which the ``file:line:``
        # parser below silently dropped as malformed.
        # knowledge-base/knowledge/issues/search_files_literal_query_contract.md
        flags = "-rn"
        if not case_sensitive:
            flags += "i"
        flags += " -F -H"

        # Escape single quotes in query
        safe_query = query.replace("'", "'\\''")

        # Exclude binary file extensions
        excludes = " ".join(
            f"--exclude='*.{ext}'"
            for ext in ["pdf", "docx", "png", "jpg", "gif", "zip", "db"]
        )
        exclude_dir_flags = ""
        if exclude_dirs:
            # Use safe shell-quoting for single quotes inside directory names:
            # Keep this format aligned with query escaping.
            exclude_dir_flags = " ".join(
                "--exclude-dir='{}'".format(d.replace("'", "'\\''"))
                for d in exclude_dirs
            )

        # The path is model-supplied: quote it as ONE word. Unquoted, a space
        # split it into extra operands resolved from the SSH user's home
        # (outside the workspace root), and ; $() ` ran commands — on tiers
        # whose experts are granted no shell tool at all.
        cmd = (
            f"grep {flags} {excludes} {exclude_dir_flags} -- '{safe_query}' "
            f"{shlex.quote(remote_path)} 2>/dev/null "
            f"| head -n {SEARCH_RESULT_HARD_CAP} || true"
        )
        output = self._exec(cmd, timeout=60)

        results = []
        for line in output.splitlines():
            if not line.strip():
                continue
            # grep -n output: /path/to/file:linenum:content
            parts = line.split(":", 2)
            if len(parts) < 3:
                continue
            file_path, line_num, content = parts[0], parts[1], parts[2]

            # Make path relative to workspace root
            if file_path.startswith(self._remote_root + "/"):
                rel_path = file_path[len(self._remote_root) + 1 :]
            else:
                rel_path = file_path

            try:
                results.append(
                    {
                        "path": rel_path,
                        "line_number": int(line_num),
                        "line": content.strip(),
                    }
                )
            except ValueError:
                continue

        return results

    def mkdir(self, path: str) -> None:
        self._ensure_connected()
        remote_path = self._resolve(path)
        self._ensure_remote_dir(remote_path)  # Already holds _sftp_lock internally
        logger.debug(f"Created remote directory: {path}")

    def delete_file(self, path: str) -> bool:
        self._ensure_connected()
        remote_path = self._resolve(path)
        st = self._remote_stat(remote_path)
        if st is None:
            return False

        with self._sftp_lock:
            if stat_module.S_ISREG(st.st_mode):
                self._sftp.remove(remote_path)
                logger.debug(f"Deleted remote file: {path}")
                return True

            if stat_module.S_ISDIR(st.st_mode):
                # Check if empty
                entries = self._sftp.listdir(remote_path)
                if entries:
                    raise ValueError(f"Cannot delete non-empty directory: {path}")
                self._sftp.rmdir(remote_path)
                logger.debug(f"Deleted remote directory: {path}")
                return True

        return False

    def delete_directory(self, path: str) -> bool:
        self._ensure_connected()
        remote_path = self._resolve(path)

        if remote_path == self._remote_root:
            raise ValueError("Cannot delete workspace root directory")

        st = self._remote_stat(remote_path)
        if st is None:
            return False
        if not stat_module.S_ISDIR(st.st_mode):
            raise ValueError(f"Not a directory: {path}")

        # Use rm -rf for recursive delete
        safe_path = remote_path.replace("'", "'\\''")
        self._exec(f"rm -rf '{safe_path}'", timeout=_HEAVY_OP_TIMEOUT_SECONDS)
        logger.debug(f"Deleted remote directory: {path}")
        return True

    def move(self, src: str, dst: str) -> None:
        self._ensure_connected()
        src_path = self._resolve(src)
        dst_path = self._resolve(dst)

        st = self._remote_stat(src_path)
        if st is None:
            raise FileNotFoundError(f"Source not found: {src}")
        # Ensure parent of destination exists
        self._ensure_remote_dir(posixpath.dirname(dst_path))

        safe_src = src_path.replace("'", "'\\''")
        safe_dst = dst_path.replace("'", "'\\''")
        _output, exit_code = self._exec_with_status(
            f"mv -- '{safe_src}' '{safe_dst}'",
            timeout=_MEDIUM_OP_TIMEOUT_SECONDS,
        )
        if exit_code != 0:
            raise OSError(
                f"Remote move failed with exit code {exit_code}: {src} -> {dst}"
            )
        logger.debug(f"Moved remote: {src} -> {dst}")

    def replace_file(self, src: str, dst: str) -> None:
        """Replace one exact path; never move the source into a directory."""

        self._ensure_connected()
        src_path = self._resolve(src)
        dst_path = self._resolve(dst)
        st = self._remote_stat(src_path)
        if st is None:
            raise FileNotFoundError(f"Source not found: {src}")
        dst_st = self._remote_stat(dst_path)
        if dst_st is not None and stat_module.S_ISDIR(dst_st.st_mode):
            raise ValueError(f"Destination is a directory: {dst}")
        self._ensure_remote_dir(posixpath.dirname(dst_path))
        safe_src = src_path.replace("'", "'\\''")
        safe_dst = dst_path.replace("'", "'\\''")
        _output, exit_code = self._exec_with_status(
            f"mv -T -- '{safe_src}' '{safe_dst}'",
            timeout=_MEDIUM_OP_TIMEOUT_SECONDS,
        )
        if exit_code != 0:
            raise OSError(
                f"Remote replace failed with exit code {exit_code}: {src} -> {dst}"
            )
        logger.debug(f"Replaced remote file: {src} -> {dst}")

    def copy(self, src: str, dst: str) -> None:
        self._ensure_connected()
        src_path = self._resolve(src)
        dst_path = self._resolve(dst)

        st = self._remote_stat(src_path)
        if st is None:
            raise FileNotFoundError(f"Source not found: {src}")
        if stat_module.S_ISDIR(st.st_mode):
            raise ValueError(f"Cannot copy directory: {src}. Use move for directories.")

        self._ensure_remote_dir(posixpath.dirname(dst_path))

        safe_src = src_path.replace("'", "'\\''")
        safe_dst = dst_path.replace("'", "'\\''")
        self._exec(
            f"cp -a '{safe_src}' '{safe_dst}'", timeout=_HEAVY_OP_TIMEOUT_SECONDS
        )
        logger.debug(f"Copied remote: {src} -> {dst}")

    def stat(self, path: str) -> int:
        self._ensure_connected()
        remote_path = self._resolve(path)
        st = self._remote_stat(remote_path)
        if st is None:
            return 0

        if stat_module.S_ISREG(st.st_mode):
            return st.st_size

        # Directory: use du -sb for total size
        safe_path = remote_path.replace("'", "'\\''")
        output = self._exec(
            f"du -sb '{safe_path}' 2>/dev/null || echo '0'",
            timeout=_MEDIUM_OP_TIMEOUT_SECONDS,
        )
        try:
            return int(output.split()[0])
        except (ValueError, IndexError):
            return 0

    # =========================================================================
    # Shell operations — remote tmux over SSH
    # =========================================================================

    def _tmux_target(self, tab_name: Optional[str] = None) -> str:
        # tmux otherwise prefix-matches target names (`foo` selects
        # `foo-extra`). Exact targets are load-bearing before any ownership
        # option is read or mutated.
        target = (
            # The trailing colon forces tmux's session-target parser. Without
            # it, commands such as `set-option -t =name` treat the leading '='
            # as a literal session character even though has-session accepts it.
            f"={self._session_name}:"
            if tab_name is None
            else f"={self._session_name}:={tab_name}"
        )
        return shlex.quote(target)

    def _tmux_pane_target(self, tab_name: str) -> str:
        tab = self._tabs.get(tab_name)
        if tab is None or not tab.pane_id:
            raise WorkspaceUnavailableError(
                f"Remote tmux tab {tab_name!r} has no attested pane identity"
            )
        return shlex.quote(tab.pane_id)

    def _tmux_state_shell(self) -> str:
        """Shell helpers for the PVC-resident stateless ownership tombstone."""
        filename = self._tmux_state_filename
        owner = self._tmux_owner_digest
        if self.workspace_incarnation_fenced:
            assert self._workspace_generation is not None
            assert self._runtime_incarnation is not None
            workspace_generation = shlex.quote(self._workspace_generation)
            runtime_incarnation = shlex.quote(self._runtime_incarnation)
            return (
                '_srw_state_dir="$HOME/.srw/tmux"\n'
                f'_srw_state="$_srw_state_dir/{filename}"\n'
                "_srw_load_state() {\n"
                '  [ -f "$_srw_state" ] || return 1\n'
                '  [ "$(wc -l < "$_srw_state")" -eq 1 ] || exit 78\n'
                "  IFS='|' read -r _srw_version _srw_owner _srw_field3 "
                "_srw_field4 _srw_field5 _srw_field6 _srw_field7 _srw_extra "
                '< "$_srw_state"\n'
                f'  [ "$_srw_owner" = {shlex.quote(owner)} ] || exit 78\n'
                '  [ -z "$_srw_extra" ] || exit 78\n'
                '  if [ "$_srw_version" = '
                f"{_TMUX_STATE_VERSION} ]; then\n"
                '    [ -z "$_srw_field6$_srw_field7" ] || exit 78\n'
                "    _srw_process_tagged=false\n"
                '    _srw_workspace_generation=""\n'
                '    _srw_runtime_incarnation=""\n'
                '    _srw_status="$_srw_field3"\n'
                '    _srw_token="$_srw_field4"\n'
                '    _srw_generation="$_srw_field5"\n'
                '  elif [ "$_srw_version" = '
                f"{_TMUX_LEGACY_INCARNATION_STATE_VERSION} ] || "
                '[ "$_srw_version" = '
                f"{_TMUX_INCARNATION_STATE_VERSION} ]; then\n"
                '    if [ "$_srw_version" = '
                f"{_TMUX_INCARNATION_STATE_VERSION} ]; then "
                "_srw_process_tagged=true; else _srw_process_tagged=false; fi\n"
                '    _srw_workspace_generation="$_srw_field3"\n'
                '    _srw_runtime_incarnation="$_srw_field4"\n'
                '    _srw_status="$_srw_field5"\n'
                '    _srw_token="$_srw_field6"\n'
                '    _srw_generation="$_srw_field7"\n'
                '    printf "%s" "$_srw_workspace_generation" | '
                "grep -Eq '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-"
                "[0-9a-f]{12}$' || exit 78\n"
                '    printf "%s" "$_srw_runtime_incarnation" | '
                "grep -Eq '^[0-9a-f]{8}(-[0-9a-f]{4}){3}-"
                "[0-9a-f]{12}$' || exit 78\n"
                "  else\n"
                "    exit 78\n"
                "  fi\n"
                '  case "$_srw_status" in active|creating|retired) ;; '
                "*) exit 78 ;; esac\n"
                '  case "$_srw_token" in ""|*[!0-9]*) exit 78 ;; esac\n'
                '  printf "%s" "$_srw_generation" | '
                "grep -Eq '^[0-9a-f]{32}$' || exit 78\n"
                "}\n"
                "_srw_write_state() {\n"
                '  _srw_tmp=$(mktemp "$_srw_state_dir/.state.XXXXXX") || exit 78\n'
                "  trap 'rm -f \"$_srw_tmp\"' EXIT HUP INT TERM\n"
                f"  printf '{_TMUX_INCARNATION_STATE_VERSION}|{owner}|"
                f"{workspace_generation}|{runtime_incarnation}|%s|%s|%s\\n' "
                '"$1" "$2" "$3" > "$_srw_tmp" || exit 78\n'
                '  chmod 600 "$_srw_tmp" || exit 78\n'
                '  mv -f "$_srw_tmp" "$_srw_state" || exit 78\n'
                "  trap - EXIT HUP INT TERM\n"
                "}\n"
            )
        return (
            '_srw_state_dir="$HOME/.srw/tmux"\n'
            f'_srw_state="$_srw_state_dir/{filename}"\n'
            "_srw_load_state() {\n"
            '  [ -f "$_srw_state" ] || return 1\n'
            '  [ "$(wc -l < "$_srw_state")" -eq 1 ] || exit 78\n'
            "  IFS='|' read -r _srw_version _srw_owner _srw_status "
            '_srw_token _srw_generation _srw_extra < "$_srw_state"\n'
            f'  [ "$_srw_version" = {_TMUX_STATE_VERSION} ] || exit 78\n'
            f'  [ "$_srw_owner" = {shlex.quote(owner)} ] || exit 78\n'
            '  [ -z "$_srw_extra" ] || exit 78\n'
            '  case "$_srw_status" in active|creating|retired) ;; '
            "*) exit 78 ;; esac\n"
            '  case "$_srw_token" in ""|*[!0-9]*) exit 78 ;; esac\n'
            '  printf "%s" "$_srw_generation" | '
            "grep -Eq '^[0-9a-f]{32}$' || exit 78\n"
            "}\n"
            "_srw_write_state() {\n"
            '  _srw_tmp=$(mktemp "$_srw_state_dir/.state.XXXXXX") || exit 78\n'
            "  trap 'rm -f \"$_srw_tmp\"' EXIT HUP INT TERM\n"
            f"  printf '{_TMUX_STATE_VERSION}|{owner}|%s|%s|%s\\n' "
            '"$1" "$2" "$3" > "$_srw_tmp" || exit 78\n'
            '  chmod 600 "$_srw_tmp" || exit 78\n'
            '  mv -f "$_srw_tmp" "$_srw_state" || exit 78\n'
            "  trap - EXIT HUP INT TERM\n"
            "}\n"
        )

    def _tmux_lock_command(self, inner: str, *, shell: str = "sh") -> str:
        """Wrap one tmux transaction in the appropriate workspace-side lock."""
        if shell not in {"sh", "bash"}:
            raise ValueError("tmux lock shell must be sh or bash")
        lock_name = self._tmux_durable_lock_filename
        return (
            # Keep the lock in flock's waiting parent, but do not let the tmux
            # server inherit the descriptor and hold it forever. Pinned and
            # stateless owners intentionally share this session-name lock.
            'umask 077; mkdir -p "$HOME/.srw/tmux" || exit 78; '
            f'flock -o -w 30 "$HOME/.srw/tmux/{lock_name}" '
            f"{shell} -c {shlex.quote(inner)}"
        )

    def _pinned_tmux_fence_shell(self) -> str:
        """Keep a tokenless pinned owner out after stateless cutover."""
        if self._shell_owner_token is not None:
            return ""
        return (
            '_srw_state="$HOME/.srw/tmux/'
            f'{self._tmux_state_filename}"\n'
            '[ ! -e "$_srw_state" ] || exit 75\n'
        )

    def _stateless_tmux_fence_shell(self) -> str:
        """Validate marker, generation, full owner, and exact lease token."""
        if self._shell_owner_token is None:
            return ""
        token = str(self._shell_owner_token)
        expected_owner = self._job_id or self._session_name
        target = self._tmux_target()
        workspace_state_check = ""
        workspace_tmux_check = ""
        if self.workspace_incarnation_fenced:
            assert self._workspace_generation is not None
            assert self._runtime_incarnation is not None
            expected_workspace = shlex.quote(self._workspace_generation)
            expected_runtime = shlex.quote(self._runtime_incarnation)
            workspace_state_check = (
                '[ "$_srw_process_tagged" = true ] || exit 81\n'
                f'[ "$_srw_workspace_generation" = {expected_workspace} ] '
                "|| exit 80\n"
                f'[ "$_srw_runtime_incarnation" = {expected_runtime} ] '
                "|| exit 80\n"
            )
            workspace_tmux_check = (
                "_srw_tmux_workspace_generation=$(tmux display-message -p "
                f"-t {target} '#{{{_TMUX_WORKSPACE_GENERATION_OPTION}}}')\n"
                f'[ "$_srw_tmux_workspace_generation" = {expected_workspace} ] '
                "|| exit 80\n"
                "_srw_tmux_runtime_incarnation=$(tmux display-message -p "
                f"-t {target} '#{{{_TMUX_RUNTIME_INCARNATION_OPTION}}}')\n"
                f'[ "$_srw_tmux_runtime_incarnation" = {expected_runtime} ] '
                "|| exit 80\n" + self._stateless_tmux_process_tag_check_shell(target)
            )
        return (
            self._tmux_state_shell()
            + "_srw_load_state || exit 78\n"
            + '[ "$_srw_process_tagged" = true ] || exit 81\n'
            + workspace_state_check
            + '[ "$_srw_status" = active ] || exit 75\n'
            + f'[ "$_srw_token" = {shlex.quote(token)} ] || exit 75\n'
            + f"tmux has-session -t {target} 2>/dev/null || exit 79\n"
            + "_srw_tmux_owner=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
            + f'[ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] || exit 73\n'
            + "_srw_tmux_token=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
            + f'[ "$_srw_tmux_token" = {shlex.quote(token)} ] || exit 75\n'
            + "_srw_tmux_generation=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_GENERATION_OPTION}}}')\n"
            + '[ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            + workspace_tmux_check
        )

    def _stateless_terminal_resource_adoption_shell(self) -> str:
        """Validate an older remote owner and monotonically adopt End's token."""

        if (
            self._shell_owner_token is None
            or self._workspace_generation is None
            or self._runtime_incarnation is None
        ):
            raise AssertionError(
                "terminal resource adoption requires token, backing, and runtime"
            )
        token = shlex.quote(str(self._shell_owner_token))
        expected_owner = shlex.quote(self._job_id or self._session_name)
        expected_workspace = shlex.quote(self._workspace_generation)
        expected_runtime = shlex.quote(self._runtime_incarnation)
        target = self._tmux_target()
        fallback_generation = shlex.quote(uuid.uuid4().hex)
        return (
            self._tmux_state_shell()
            + "if _srw_load_state; then\n"
            + '  [ "$_srw_process_tagged" = true ] || exit 81\n'
            + f'  if [ "$_srw_workspace_generation" = {expected_workspace} ] && '
            + f'[ "$_srw_runtime_incarnation" = {expected_runtime} ]; then\n'
            + "    _srw_marker=present\n"
            + '    [ "$_srw_status" = active ] || '
            + '[ "$_srw_status" = creating ] || exit 75\n'
            + f'    [ "$_srw_token" -le {token} ] || exit 75\n'
            + "  else\n"
            # The PVC-resident record of a retired predecessor runtime: a
            # Resume reuses the volume, and only a new turn rewrites it.
            # Mirror shell_cleanup's stale branch: never behind End's token,
            # never beside a same-name tmux it cannot explain, otherwise
            # re-stamped below under this exact runtime.
            + f'    [ "$_srw_token" -le {token} ] || exit 75\n'
            + f"    ! tmux has-session -t {target} 2>/dev/null || exit 80\n"
            + "    _srw_marker=absent\n"
            + f"    _srw_generation={fallback_generation}\n"
            + "  fi\n"
            + "else\n"
            + '  _srw_rc=$?; [ "$_srw_rc" -eq 1 ] || exit "$_srw_rc"\n'
            + "  _srw_marker=absent\n"
            + f"  _srw_generation={fallback_generation}\n"
            + "fi\n"
            + f"if tmux has-session -t {target} 2>/dev/null; then\n"
            + '  [ "$_srw_marker" = present ] || exit 79\n'
            + "  _srw_tmux_owner=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
            + f'  [ "$_srw_tmux_owner" = {expected_owner} ] || exit 73\n'
            + "  _srw_tmux_token=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
            + '  case "$_srw_tmux_token" in "" ) _srw_tmux_token=0 ;; '
            + "*[!0-9]* ) exit 76 ;; esac\n"
            + f'  [ "$_srw_tmux_token" -le {token} ] || exit 75\n'
            + "  _srw_tmux_generation=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_GENERATION_OPTION}}}')\n"
            + '  [ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            + "  _srw_tmux_workspace_generation=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_WORKSPACE_GENERATION_OPTION}}}')\n"
            + f'  [ "$_srw_tmux_workspace_generation" = {expected_workspace} ] '
            + "|| exit 80\n"
            + "  _srw_tmux_runtime_incarnation=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_RUNTIME_INCARNATION_OPTION}}}')\n"
            + f'  [ "$_srw_tmux_runtime_incarnation" = {expected_runtime} ] '
            + "|| exit 80\n"
            + self._stateless_tmux_process_tag_check_shell(target, indent="  ")
            + f"  tmux set-option -t {target} {_TMUX_OWNER_TOKEN_OPTION} "
            + f"{token} || exit 77\n"
            + 'elif [ "$_srw_marker" = absent ]; then\n'
            + "  :\n"
            + "fi\n"
            + f'_srw_write_state active {token} "$_srw_generation"\n'
        )

    def _stateless_retired_resource_fence_shell(self) -> str:
        """Require the exact terminal tombstone and prove tmux remains absent."""

        if (
            self._shell_owner_token is None
            or self._workspace_generation is None
            or self._runtime_incarnation is None
        ):
            raise AssertionError(
                "retired resource verification requires token, backing, and runtime"
            )
        token = shlex.quote(str(self._shell_owner_token))
        expected_workspace = shlex.quote(self._workspace_generation)
        expected_runtime = shlex.quote(self._runtime_incarnation)
        target = self._tmux_target()
        return (
            self._tmux_state_shell()
            + "_srw_load_state || exit 78\n"
            + '[ "$_srw_process_tagged" = true ] || exit 81\n'
            + f'[ "$_srw_workspace_generation" = {expected_workspace} ] '
            + "|| exit 80\n"
            + f'[ "$_srw_runtime_incarnation" = {expected_runtime} ] '
            + "|| exit 80\n"
            + '[ "$_srw_status" = retired ] || exit 75\n'
            + f'[ "$_srw_token" = {token} ] || exit 75\n'
            + f"! tmux has-session -t {target} 2>/dev/null || exit 79\n"
            + self._stateless_terminal_process_zero_shell(terminate=False)
        )

    def _close_sftp_before_process_zero_proof(self) -> None:
        """Close this backend's own SFTP channel before a process-zero proof.

        The stateless process-zero scan intentionally refuses unreadable
        same-UID processes. ``connect()`` opens an SFTP subsystem whose server
        is a sibling of the exec channel, not an ancestor the scan can
        exclude, and OpenSSH's ``sftp-server`` makes itself non-dumpable, so
        the workspace user cannot inspect it. Close this terminal backend's
        own writer channel before opening the proof exec; the still-active
        SSH transport is sufficient for that exec and ``_ensure_connected()``
        will not recreate SFTP.
        """
        self._ensure_connected()
        with self._sftp_lock:
            sftp = self._sftp
            self._sftp = None
            if sftp is not None:
                sftp.close()

    def _tmux_exec_checked(
        self,
        command: str,
        *,
        operation: str,
        allow_shell_retired: bool = False,
        close_sftp: bool = False,
        pane_context: Optional[tuple[str, str]] = None,
    ) -> str:
        """Run a generated tmux command and prove its remote exit status.

        ``_exec`` intentionally returns stdout for non-zero remote commands;
        many file probes rely on that behavior. Shell control is different: a
        missing session/window must never be mistaken for a successful durable
        guard or key send. Use Paramiko's already-drained remote exit status
        directly, without adding temp-file I/O to the 300 ms capture loop.
        Pane-scoped callers may supply their exact tab and pane identity;
        only a follow-up liveness proof can then downgrade the failure to an
        ordinary missing-tab error.
        """
        with self._shell_io_lock:
            if self._shell_retired and not allow_shell_retired:
                raise WorkspaceUnavailableError(
                    "Remote shell owner has been retired from this backend"
                )
            if close_sftp:
                self._close_sftp_before_process_zero_proof()
            # tmux completion sentinels and the attested prompt live at the end
            # of capture-pane output. Keep a bounded tail so scrollback over the
            # generic 5 MiB SSH cap cannot make a finished command look busy.
            output, exit_code = self._exec_with_status(command, retain_tail=True)
            if exit_code != 0:
                if exit_code == 1 and pane_context is not None:
                    # Fence failures use reserved exit codes. A vanished pane
                    # must never turn a stale owner into a recoverable tab error.
                    tab_name, pane_id = pane_context
                    try:
                        pane_gone = self._tmux_pane_gone(tab_name, pane_id)
                    except Exception as probe_error:
                        self._shell_initialized = False
                        self._tabs.clear()
                        raise WorkspaceUnavailableError(
                            f"Remote tmux {operation} failed with exit code {exit_code}"
                        ) from probe_error
                    if pane_gone:
                        self._prune_lost_tmux_tab(tab_name)
                        raise KeyError(self._lost_tab_message(tab_name))
                self._shell_initialized = False
                self._tabs.clear()
                raise WorkspaceUnavailableError(
                    f"Remote tmux {operation} failed with exit code {exit_code}"
                )
            return output

    def _lost_tab_message(self, tab_name: str) -> str:
        return (
            f"Tab '{tab_name}' no longer exists. "
            f"Available: {', '.join(self._tabs.keys())}"
        )

    def _tmux_pane_gone(self, tab_name: str, pane_id: str) -> bool:
        """Prove that one pane vanished while its exact session stayed live.

        Tmux stderr is intentionally not part of the decision. A successful
        session probe proves that the server and exact SRW session answered;
        the exact window's pane inventory is then authoritative. Re-check the
        session after a failed window lookup so a concurrent session loss
        remains a workspace-level failure.
        """
        session_probe = f"tmux has-session -t {self._tmux_target()} >/dev/null 2>&1"
        _output, session_exit = self._exec_with_status(session_probe)
        if session_exit != 0:
            return False

        panes, panes_exit = self._exec_with_status(
            f"tmux list-panes -t {self._tmux_target(tab_name)} "
            "-F '#{pane_id}' 2>/dev/null"
        )
        if panes_exit == 0:
            pane_ids = {line.strip() for line in panes.splitlines() if line.strip()}
            return pane_id not in pane_ids

        _output, session_exit = self._exec_with_status(session_probe)
        return session_exit == 0

    def _prune_lost_tmux_tab(self, tab_name: str) -> None:
        """Remove one proven-lost pane without weakening tmux owner fences."""
        session = self._tmux_target()
        window = self._tmux_target(tab_name)
        clear_options = "".join(
            f"  tmux set-option -w -u -t {window} {option} 2>/dev/null || true\n"
            for option in _TMUX_WINDOW_STATE_OPTIONS
        )
        cleanup = (
            f"tmux has-session -t {session} 2>/dev/null || exit 79\n"
            f"if tmux list-panes -t {window} >/dev/null 2>&1; then\n"
            f"{clear_options}"
            f"  if ! tmux kill-window -t {window} 2>/dev/null; then\n"
            f"    tmux has-session -t {session} 2>/dev/null || exit 79\n"
            "  fi\n"
            "else\n"
            f"  tmux has-session -t {session} 2>/dev/null || exit 79\n"
            "fi"
        )
        self._tmux_mutate_checked(
            cleanup,
            operation=f"prune lost pane {tab_name}",
        )
        self._tabs.pop(tab_name, None)
        logger.warning(
            "pane_lost_at_handoff: pruned dead remote tmux pane: "
            "session=%s tab=%s shell_owner_token=%s",
            self._session_name,
            tab_name,
            self._shell_owner_token,
        )

    def _tmux_mutate_checked(
        self,
        command: str,
        *,
        operation: str,
        allow_missing_session: bool = False,
        pane_context: Optional[tuple[str, str]] = None,
    ) -> str:
        """Serialize a tmux mutation and fence stale stateless claimants."""
        if allow_missing_session and self._shell_owner_token is not None:
            raise ValueError(
                "stateless tmux mutations cannot bypass the durable session fence"
            )
        inner = (
            f"{self._pinned_tmux_fence_shell()}"
            f"{self._stateless_tmux_fence_shell()}"
            f"{command}"
        )
        return self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation=operation,
            pane_context=pane_context,
        )

    def _promote_tmux_owner_token(self) -> None:
        """Assert the token installed atomically by create-or-observe."""
        if self._shell_owner_token is None:
            return
        self._tmux_mutate_checked(
            ":",
            operation="attest shell owner token",
        )

    def _set_tmux_window_option(self, tab_name: str, option: str, value: str) -> None:
        self._tmux_mutate_checked(
            "tmux set-option -w "
            f"-t {self._tmux_target(tab_name)} {option} {shlex.quote(value)}",
            operation=f"set {option} on {tab_name}",
        )

    def _clear_tmux_window_option(self, tab_name: str, option: str) -> None:
        target = self._tmux_target(tab_name)
        self._tmux_mutate_checked(
            f"tmux display-message -p -t {target} '#{{window_id}}' >/dev/null "
            f"&& (tmux set-option -w -u -t {target} {option} 2>/dev/null || true)",
            operation=f"clear {option} on {tab_name}",
        )

    def _set_tmux_session_option(self, option: str, value: str) -> None:
        self._tmux_mutate_checked(
            f"tmux set-option -t {self._tmux_target()} {option} {shlex.quote(value)}",
            operation=f"set session option {option}",
        )

    def _read_tmux_session_option(self, option: str) -> str:
        return self._tmux_exec_checked(
            f"tmux display-message -p -t {self._tmux_target()} "
            f"{shlex.quote(f'#{{{option}}}')}",
            operation=f"read session option {option}",
        ).strip()

    def _pending_guard_cas_shell(self, pane: str, expected_value: str) -> str:
        """CAS the durable pane guard, aging out abandoned reservations.

        On mismatch against a *present* guard, consult its epoch stamp: a
        guard older than ``_PENDING_GUARD_STALE_SECONDS`` is abandoned (its
        owner would have CAS-cleared it or still hold it in a local registry
        whose pre-flight blocks before this CAS runs), so clear it, report it
        for logging, and let the reservation proceed. A guard with no stamp
        (written by pre-aging code) or a stamp from a regressed clock gets
        stamped ``now`` and refused; it then ages out normally, so a legacy
        guard keeps one full bound of collision protection before recovery.
        Both stamp and comparison use the workspace clock (``date +%s``), so
        local/remote skew cannot forge or mask staleness.
        """
        return (
            "_srw_pending=$(tmux display-message -p "
            f"-t {pane} '#{{{_TMUX_PENDING_SENTINEL_OPTION}}}')\n"
            f'if [ "$_srw_pending" != {shlex.quote(expected_value)} ]; then\n'
            '  [ -n "$_srw_pending" ] || exit 74\n'
            "  _srw_now=$(date +%s)\n"
            "  _srw_since=$(tmux display-message -p "
            f"-t {pane} '#{{{_TMUX_PENDING_SINCE_OPTION}}}')\n"
            '  case "$_srw_since" in\n'
            "    ''|*[!0-9]*) _srw_since='' ;;\n"
            "  esac\n"
            '  if [ -z "$_srw_since" ] || [ "$_srw_now" -lt "$_srw_since" ]; '
            "then\n"
            f"    tmux set-option -w -t {pane} {_TMUX_PENDING_SINCE_OPTION} "
            '"$_srw_now"\n'
            "    exit 74\n"
            "  fi\n"
            "  _srw_age=$((_srw_now - _srw_since))\n"
            f'  [ "$_srw_age" -ge {_PENDING_GUARD_STALE_SECONDS} ] || exit 74\n'
            f"  printf '{_STALE_GUARD_CLEARED_MARKER} %s %s\\n' "
            '"$_srw_age" "$_srw_pending"\n'
            "fi\n"
        )

    def _pending_guard_set_shell(self, pane: str, sentinel: str) -> str:
        """Install the durable guard together with its freshness stamp."""
        return (
            f"tmux set-option -w -t {pane} {_TMUX_PENDING_SENTINEL_OPTION} "
            f"{shlex.quote(sentinel)}\n"
            f"tmux set-option -w -t {pane} {_TMUX_PENDING_SINCE_OPTION} "
            '"$(date +%s)"'
        )

    def _warn_if_stale_guard_cleared(self, tab_name: str, output: str) -> None:
        for line in output.splitlines():
            parts = line.strip().split(maxsplit=2)
            if (
                len(parts) == 3
                and parts[0] == _STALE_GUARD_CLEARED_MARKER
                and parts[1].isdigit()
            ):
                logger.warning(
                    "Cleared stale remote tmux pending guard: session=%s "
                    "tab=%s stale_value=%s age_seconds=%s (stale after %s s)",
                    self._session_name,
                    tab_name,
                    parts[2],
                    parts[1],
                    _PENDING_GUARD_STALE_SECONDS,
                )

    def _set_tab_pending(
        self,
        tab_name: str,
        sentinel: str,
        *,
        expected: Optional[str] = None,
    ) -> None:
        pane = self._tmux_pane_target(tab_name)
        expected_value = expected or ""
        output = self._tmux_mutate_checked(
            self._pending_guard_cas_shell(pane, expected_value)
            + self._pending_guard_set_shell(pane, sentinel),
            operation=f"reserve pending state on {tab_name}",
        )
        self._warn_if_stale_guard_cleared(tab_name, output)
        tab = self._tabs.get(tab_name)
        if tab is None:
            # `_exec` may have reconnected the SSH transport and invalidated
            # the local registry while setting the durable guard. No command
            # has been typed yet, so fail safely; reattach will observe the
            # guard and clear it once it sees the existing prompt.
            raise WorkspaceUnavailableError(
                "Remote shell transport changed while reserving tab "
                f"{tab_name!r}; retry after reattach"
            )
        tab.pending_sentinel = sentinel

    def _clear_tab_pending(self, tab_name: str, expected: Optional[str]) -> None:
        if expected is None:
            return
        pane = self._tmux_pane_target(tab_name)
        self._tmux_mutate_checked(
            "_srw_pending=$(tmux display-message -p "
            f"-t {pane} '#{{{_TMUX_PENDING_SENTINEL_OPTION}}}')\n"
            f'[ "$_srw_pending" = {shlex.quote(expected)} ] || exit 74\n'
            f"tmux set-option -w -u -t {pane} "
            f"{_TMUX_PENDING_SENTINEL_OPTION} 2>/dev/null || true\n"
            # Drop the stamp with the guard: a leftover old stamp would make
            # the next guard written by pre-aging code look instantly stale.
            f"tmux set-option -w -u -t {pane} "
            f"{_TMUX_PENDING_SINCE_OPTION} 2>/dev/null || true",
            operation=f"clear pending state on {tab_name}",
        )
        # A transport reconnect clears the local tab registry so the next
        # operation rehydrates it from tmux. The command that detected its own
        # completion may still be unwinding at that point; clearing the durable
        # option remains authoritative and must not turn into a local KeyError.
        tab = self._tabs.get(tab_name)
        if tab is not None:
            tab.pending_sentinel = None

    def _clear_tab_pending_if_current(
        self,
        tab_name: str,
        expected: Optional[str],
    ) -> None:
        """CAS-clear only when a helper has not already replaced the guard."""
        if expected is None:
            return
        tab = self._tabs.get(tab_name)
        if tab is None or tab.pending_sentinel != expected:
            return
        self._clear_tab_pending(tab_name, expected)

    def _reserve_and_send_shell_command(
        self,
        tab_name: str,
        *,
        expected: Optional[str],
        sentinel: str,
        command: str,
    ) -> None:
        """CAS the pane guard and type a command under one remote lock."""
        pane = self._tmux_pane_target(tab_name)
        expected_value = expected or ""
        output = self._tmux_mutate_checked(
            self._pending_guard_cas_shell(pane, expected_value)
            + self._pending_guard_set_shell(pane, sentinel)
            + f"\ntmux send-keys -t {pane} -l {shlex.quote(command)}\n"
            f"tmux send-keys -t {pane} Enter",
            operation=f"reserve and send command on {tab_name}",
        )
        self._warn_if_stale_guard_cleared(tab_name, output)
        tab = self._tabs.get(tab_name)
        if tab is None:
            raise WorkspaceUnavailableError(
                "Remote shell transport changed while sending command on "
                f"{tab_name!r}; reattach before retrying"
            )
        tab.pending_sentinel = sentinel

    def _cancel_and_probe_shell_command(
        self,
        tab_name: str,
        *,
        expected: str,
        sentinel: str,
    ) -> None:
        """Replace a guard, send C-c, then queue a completion probe atomically."""
        pane = self._tmux_pane_target(tab_name)
        probe = f"printf '\\n{sentinel} 130 %s\\n' \"$PWD\""
        # Cancel targets one specific known guard; a mismatch means someone
        # else owns the pane now, so the CAS stays strict (no stale aging —
        # interrupting an unknown owner is never a recovery).
        self._tmux_mutate_checked(
            "_srw_pending=$(tmux display-message -p "
            f"-t {pane} '#{{{_TMUX_PENDING_SENTINEL_OPTION}}}')\n"
            f'[ "$_srw_pending" = {shlex.quote(expected)} ] || exit 74\n'
            + self._pending_guard_set_shell(pane, sentinel)
            + f"\ntmux send-keys -t {pane} C-c\n"
            f"tmux send-keys -t {pane} -l {shlex.quote(probe)}\n"
            f"tmux send-keys -t {pane} Enter",
            operation=f"cancel and probe command on {tab_name}",
        )
        tab = self._tabs.get(tab_name)
        if tab is None:
            raise WorkspaceUnavailableError(
                "Remote shell transport changed while cancelling command on "
                f"{tab_name!r}"
            )
        tab.pending_sentinel = sentinel

    def _stateless_tmux_create_shell(self, token: str, generation: str) -> str:
        """Return the locked shell fragment that creates one fenced session."""
        session = self._tmux_target()
        default_window = self._tmux_target("default")
        raw_session_name = shlex.quote(self._session_name)
        expected_owner = shlex.quote(self._job_id or self._session_name)
        workspace_option = ""
        creation_env = ""
        if self.workspace_incarnation_fenced:
            assert self._workspace_generation is not None
            assert self._runtime_incarnation is not None
            workspace_option = (
                f"tmux set-option -t {session} "
                f"{_TMUX_WORKSPACE_GENERATION_OPTION} "
                f"{shlex.quote(self._workspace_generation)} || exit 77\n"
                f"tmux set-option -t {session} "
                f"{_TMUX_RUNTIME_INCARNATION_OPTION} "
                f"{shlex.quote(self._runtime_incarnation)} || exit 77\n"
                f"tmux set-option -t {session} {_TMUX_PROCESS_TAG_OPTION} "
                f"{shlex.quote(self._shell_process_tag(generation))} || exit 77\n"
                f"tmux set-environment -t {session} {_WORKSPACE_PROCESS_TAG_ENV} "
                f"{shlex.quote(self._workspace_process_tag())} || exit 77\n"
                f"tmux set-environment -t {session} {_SHELL_PROCESS_TAG_ENV} "
                f"{shlex.quote(self._shell_process_tag(generation))} || exit 77\n"
            )
            # tmux must inject both tags into the *first* pane at exec time.
            # Setting the session environment afterward leaves a .bashrc
            # startup-hook interval in which a disowned child is untagged.
            creation_env = (
                f"-e {_WORKSPACE_PROCESS_TAG_ENV}="
                f"{shlex.quote(self._workspace_process_tag())} "
                f"-e {_SHELL_PROCESS_TAG_ENV}="
                f"{shlex.quote(self._shell_process_tag(generation))} "
            )
        return (
            f"_srw_write_state creating {shlex.quote(token)} "
            f"{shlex.quote(generation)}\n"
            f"tmux new-session -d -s {raw_session_name} "
            f"{creation_env}-x 200 -y 30 -n default || exit 77\n"
            f"tmux set-option -t {session} {_TMUX_OWNER_ID_OPTION} "
            f"{expected_owner} || exit 77\n"
            f"tmux set-option -t {session} {_TMUX_OWNER_TOKEN_OPTION} "
            f"{shlex.quote(token)} || exit 77\n"
            f"tmux set-option -t {session} {_TMUX_GENERATION_OPTION} "
            f"{shlex.quote(generation)} || exit 77\n"
            f"{workspace_option}"
            f"tmux set-option -t {session} {_TMUX_SETUP_OPTION} "
            f"{_TMUX_SETUP_PENDING} || exit 77\n"
            f"tmux set-option -w -t {default_window} {_TMUX_WINDOW_SETUP_OPTION} "
            f"{_TMUX_SETUP_PENDING} || exit 77\n"
            f"_srw_write_state active {shlex.quote(token)} "
            f"{shlex.quote(generation)}\n"
            "printf created"
        )

    def _stateless_create_or_observe_tmux_session(self) -> str:
        """Create/adopt tmux under a PVC-resident token+generation fence."""
        if self._shell_owner_token is None:
            raise AssertionError("stateless tmux ownership requires a lease token")
        if self.workspace_incarnation_fenced:
            return self._incarnation_create_or_observe_tmux_session()

        token = str(self._shell_owner_token)
        desired_generation = uuid.uuid4().hex
        target = self._tmux_target()
        expected_owner = self._job_id or self._session_name
        create_shell = self._stateless_tmux_create_shell(token, desired_generation)
        inspect_tmux = (
            "_srw_tmux_owner=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
            "_srw_tmux_token=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
            'case "$_srw_tmux_token" in "" ) _srw_tmux_token=0 ;; '
            "*[!0-9]* ) exit 76 ;; esac\n"
            "_srw_tmux_generation=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_GENERATION_OPTION}}}')\n"
        )
        validate_generation = (
            'printf "%s" "$_srw_tmux_generation" | '
            "grep -Eq '^[0-9a-f]{32}$' || exit 79\n"
        )

        inner = (
            self._tmux_state_shell() + "if _srw_load_state; then _srw_marker=present; "
            'else _srw_rc=$?; [ "$_srw_rc" -eq 1 ] || exit "$_srw_rc"; '
            "_srw_marker=absent; fi\n"
            'if [ "$_srw_marker" = present ]; then\n'
            '  if [ "$_srw_status" = active ]; then\n'
            f"    tmux has-session -t {target} 2>/dev/null || exit 79\n"
            f'    [ "$_srw_token" -le {shlex.quote(token)} ] || exit 75\n'
            + inspect_tmux
            + f'    [ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] '
            "|| exit 73\n"
            f'    [ "$_srw_tmux_token" -le {shlex.quote(token)} ] || exit 75\n'
            '    [ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            f"    tmux set-option -t {target} {_TMUX_OWNER_TOKEN_OPTION} "
            f"{shlex.quote(token)} || exit 77\n"
            f"    _srw_write_state active {shlex.quote(token)} "
            '"$_srw_generation"\n'
            "    printf existing\n"
            '  elif [ "$_srw_status" = creating ]; then\n'
            f'    [ {shlex.quote(token)} -ge "$_srw_token" ] || exit 75\n'
            f"    if tmux has-session -t {target} 2>/dev/null; then\n"
            + inspect_tmux
            + f'      [ -z "$_srw_tmux_owner" ] || '
            f'[ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] '
            "|| exit 73\n"
            + f'      [ "$_srw_tmux_token" -le {shlex.quote(token)} ] || exit 75\n'
            '      [ -z "$_srw_tmux_generation" ] || '
            '[ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            f"      tmux kill-session -t {target} || exit 77\n"
            "    fi\n" + create_shell + "\n"
            "  else\n"
            f'    [ {shlex.quote(token)} -gt "$_srw_token" ] || exit 75\n'
            f"    if tmux has-session -t {target} 2>/dev/null; then\n"
            + inspect_tmux
            + f'      [ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] '
            "|| exit 73\n"
            + f'      [ "$_srw_tmux_token" -le {shlex.quote(token)} ] || exit 75\n'
            + validate_generation
            + '      [ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            f"      tmux kill-session -t {target} || exit 77\n"
            "    fi\n" + create_shell + "\n  fi\n"
            "else\n"
            f"  tmux has-session -t {target} 2>/dev/null && exit 79\n"
            + create_shell
            + "\n"
            "fi"
        )
        output = self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation="create or observe stateless session",
        ).strip()
        if output not in {"created", "existing"}:
            raise WorkspaceUnavailableError(
                f"Could not create or observe remote tmux session {self._session_name}"
            )
        return output

    def _incarnation_create_or_observe_tmux_session(self) -> str:
        """Create/adopt tmux under queue and authoritative-runtime fences.

        The workspace generation identifies the durable backing and remains
        stable across a same-PVC pod replacement.  The separate runtime
        incarnation is the Kubernetes Pod UID.  A new pod therefore cannot
        satisfy the old pod's ownership record even though that record survives
        on the shared PVC.  A stale record can be superseded only when the
        current runtime has no same-named tmux session; a conflicting live
        session is ambiguous and fails closed.

        Protocol-v1 and protocol-v2 records predate mandatory inherited
        process tags. They are refused with exit 81 without mutation, whether
        active or retired; proving a safe live-process migration requires a
        separate explicitly authorized protocol.
        """
        if (
            self._shell_owner_token is None
            or self._workspace_generation is None
            or self._runtime_incarnation is None
        ):
            raise AssertionError(
                "incarnation-fenced tmux ownership requires token, backing, and runtime"
            )

        token = str(self._shell_owner_token)
        desired_generation = uuid.uuid4().hex
        target = self._tmux_target()
        expected_owner = self._job_id or self._session_name
        expected_workspace = self._workspace_generation
        expected_runtime = self._runtime_incarnation
        create_shell = self._stateless_tmux_create_shell(token, desired_generation)
        inspect_tmux = (
            "_srw_tmux_owner=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
            "_srw_tmux_token=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
            'case "$_srw_tmux_token" in "" ) _srw_tmux_token=0 ;; '
            "*[!0-9]* ) exit 76 ;; esac\n"
            "_srw_tmux_generation=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_GENERATION_OPTION}}}')\n"
            "_srw_tmux_workspace_generation=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_WORKSPACE_GENERATION_OPTION}}}')\n"
            "_srw_tmux_runtime_incarnation=$(tmux display-message -p "
            f"-t {target} '#{{{_TMUX_RUNTIME_INCARNATION_OPTION}}}')\n"
        )
        exact_owner = (
            f'[ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] || exit 73\n'
        )
        current_or_older_token = (
            f'[ "$_srw_tmux_token" -le {shlex.quote(token)} ] || exit 75\n'
        )
        exact_generation = (
            '[ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
        )
        exact_incarnation = (
            f'[ "$_srw_tmux_workspace_generation" = '
            f"{shlex.quote(expected_workspace)} ] || exit 80\n"
            f'[ "$_srw_tmux_runtime_incarnation" = '
            f"{shlex.quote(expected_runtime)} ] || exit 80\n"
        )

        current_record = (
            '  [ "$_srw_process_tagged" = true ] || exit 81\n'
            '  if [ "$_srw_status" = active ]; then\n'
            f'    [ "$_srw_token" -le {shlex.quote(token)} ] || exit 75\n'
            f"    if tmux has-session -t {target} 2>/dev/null; then\n"
            + "      "
            + inspect_tmux.replace("\n", "\n      ").rstrip()
            + "\n      "
            + exact_owner
            + "      "
            + current_or_older_token
            + "      "
            + exact_generation
            + "      "
            + exact_incarnation
            + f"      tmux set-option -t {target} {_TMUX_OWNER_TOKEN_OPTION} "
            f"{shlex.quote(token)} || exit 77\n"
            f"      _srw_write_state active {shlex.quote(token)} "
            '"$_srw_generation"\n'
            "      printf existing\n"
            "    else\n      " + create_shell.replace("\n", "\n      ") + "\n    fi\n"
            '  elif [ "$_srw_status" = creating ]; then\n'
            f'    [ {shlex.quote(token)} -ge "$_srw_token" ] || exit 75\n'
            f"    if tmux has-session -t {target} 2>/dev/null; then\n"
            + "      "
            + inspect_tmux.replace("\n", "\n      ").rstrip()
            + "\n"
            f'      [ -z "$_srw_tmux_owner" ] || '
            f'[ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] || exit 73\n'
            + "      "
            + current_or_older_token
            + '      [ -z "$_srw_tmux_generation" ] || '
            '[ "$_srw_tmux_generation" = "$_srw_generation" ] || exit 79\n'
            + '      [ -z "$_srw_tmux_workspace_generation" ] || '
            f'[ "$_srw_tmux_workspace_generation" = '
            f"{shlex.quote(expected_workspace)} ] || exit 80\n"
            + '      [ -z "$_srw_tmux_runtime_incarnation" ] || '
            f'[ "$_srw_tmux_runtime_incarnation" = '
            f"{shlex.quote(expected_runtime)} ] || exit 80\n"
            f"      tmux kill-session -t {target} || exit 77\n"
            "    fi\n    " + create_shell.replace("\n", "\n    ") + "\n"
            "  else\n"
            f'    [ {shlex.quote(token)} -gt "$_srw_token" ] || exit 75\n'
            f"    if tmux has-session -t {target} 2>/dev/null; then\n"
            + "      "
            + inspect_tmux.replace("\n", "\n      ").rstrip()
            + "\n      "
            + exact_owner
            + "      "
            + current_or_older_token
            + "      "
            + exact_generation
            + "      "
            + exact_incarnation
            + f"      tmux kill-session -t {target} || exit 77\n"
            "    fi\n    " + create_shell.replace("\n", "\n    ") + "\n  fi\n"
        )

        legacy_record = "  exit 81\n"

        inner = (
            self._tmux_state_shell() + "if _srw_load_state; then _srw_marker=present; "
            'else _srw_rc=$?; [ "$_srw_rc" -eq 1 ] || exit "$_srw_rc"; '
            "_srw_marker=absent; fi\n"
            'if [ "$_srw_marker" = present ] && '
            f'[ "$_srw_workspace_generation" = '
            f"{shlex.quote(expected_workspace)} ] && "
            f'[ "$_srw_runtime_incarnation" = '
            f"{shlex.quote(expected_runtime)} ]; then\n"
            + current_record
            + 'elif [ "$_srw_marker" = present ] && '
            '[ -z "$_srw_workspace_generation" ] && '
            '[ -z "$_srw_runtime_incarnation" ]; then\n'
            + legacy_record
            + 'elif [ "$_srw_marker" = present ]; then\n'
            # A live same-named tmux in the new authoritative runtime cannot be
            # explained by the stale record. Never kill or adopt it implicitly.
            f'  [ "$_srw_token" -le {shlex.quote(token)} ] || exit 75\n'
            f"  if tmux has-session -t {target} 2>/dev/null; then exit 80; fi\n  "
            + create_shell.replace("\n", "\n  ")
            + "\nelse\n"
            # Marker deletion cannot authorize adoption of an unmarked session.
            f"  if tmux has-session -t {target} 2>/dev/null; then exit 79; fi\n  "
            + create_shell.replace("\n", "\n  ")
            + "\nfi"
        )
        output = self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation="create or observe incarnation-fenced stateless session",
        ).strip()
        if output not in {"created", "existing"}:
            raise WorkspaceUnavailableError(
                f"Could not create or observe remote tmux session {self._session_name}"
            )
        return output

    def _reset_stateless_tmux_session(self) -> None:
        """Atomically retire and recreate this claim's incomplete/stuck shell."""
        if self._shell_owner_token is None:
            raise AssertionError("stateless tmux reset requires a lease token")
        token = str(self._shell_owner_token)
        generation = uuid.uuid4().hex
        target = self._tmux_target()
        inner = (
            self._stateless_tmux_fence_shell()
            + f"_srw_write_state creating {shlex.quote(token)} "
            '"$_srw_generation"\n'
            + f"tmux kill-session -t {target} || exit 77\n"
            + self._stateless_tmux_create_shell(token, generation)
        )
        output = self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation="reset stateless shell session",
        ).strip()
        if output != "created":
            raise WorkspaceUnavailableError(
                f"Could not reset remote tmux session {self._session_name}"
            )
        self._shell_initialized = False
        self._tabs.clear()
        self._prompt_token = None
        self._prompt_marker = None
        self._shell_generation = None
        self._shell_protocol_current = False

    def _create_or_observe_tmux_session(self) -> str:
        """Atomically create the deterministic session or observe its winner."""
        if self._shell_owner_token is not None:
            return self._stateless_create_or_observe_tmux_session()
        session = self._tmux_target()
        raw_session_name = shlex.quote(self._session_name)
        default_window = self._tmux_target("default")
        creation_env = ""
        identity_options = ""
        if self.workspace_incarnation_fenced:
            # Install at pane exec time, not as a later setup keystroke: a
            # .bashrc hook can fork a detached writer before the first prompt.
            assert self._workspace_generation is not None
            assert self._runtime_incarnation is not None
            self._shell_generation = uuid.uuid4().hex
            shell_process_tag = self._shell_process_tag(self._shell_generation)
            creation_env = (
                f"-e {_WORKSPACE_PROCESS_TAG_ENV}="
                f"{shlex.quote(self._workspace_process_tag())} "
                f"-e {_SHELL_PROCESS_TAG_ENV}="
                f"{shlex.quote(shell_process_tag)} "
            )
            identity_options = (
                f"\\; set-option -t {session} {_TMUX_WORKSPACE_GENERATION_OPTION} "
                f"{shlex.quote(self._workspace_generation)} "
                f"\\; set-option -t {session} {_TMUX_RUNTIME_INCARNATION_OPTION} "
                f"{shlex.quote(self._runtime_incarnation)} "
                f"\\; set-option -t {session} {_TMUX_GENERATION_OPTION} "
                f"{shlex.quote(self._shell_generation)} "
                f"\\; set-option -t {session} {_TMUX_PROCESS_TAG_OPTION} "
                f"{shlex.quote(shell_process_tag)} "
            )
        inner = (
            self._pinned_tmux_fence_shell()
            # Keep the established pinned-lane contract: a fresh backend owns
            # a fresh shell. Reattach is only safe under the stateless lease
            # token + durable generation fence above. Killing first also makes
            # a retry converge after interruption at any creation boundary.
            + f"tmux kill-session -t {session} 2>/dev/null || true\n"
            + f"tmux new-session -d -s {raw_session_name} {creation_env}"
            "-x 200 -y 30 -n default "
            + identity_options
            + f"\\; set-option -t {session} {_TMUX_SETUP_OPTION} "
            f"{_TMUX_SETUP_PENDING} "
            f"\\; set-option -w -t {default_window} "
            f"{_TMUX_WINDOW_SETUP_OPTION} {_TMUX_SETUP_PENDING} "
            "|| exit 77\nprintf created"
        )
        output = self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation="create pinned session",
        ).strip()
        if output != "created":
            raise WorkspaceUnavailableError(
                f"Could not create remote tmux session {self._session_name}"
            )
        return output

    def _attest_tmux_owner(self) -> None:
        """Bind the truncated tmux name to the full durable thread/job id."""
        expected = self._job_id or self._session_name
        stored = self._read_tmux_session_option(_TMUX_OWNER_ID_OPTION)
        if stored and stored != expected:
            raise WorkspaceUnavailableError(
                "Remote tmux owner identity does not match this workspace claim"
            )
        if not stored:
            self._set_tmux_session_option(_TMUX_OWNER_ID_OPTION, expected)

    def _ensure_prompt_token(self) -> None:
        stored = self._read_tmux_session_option(_TMUX_PROMPT_TOKEN_OPTION)
        if stored and not _TMUX_PROMPT_TOKEN_PATTERN.fullmatch(stored):
            raise WorkspaceUnavailableError(
                "Remote tmux prompt identity is malformed; refusing adoption"
            )
        if not stored:
            stored = uuid.uuid4().hex
            self._set_tmux_session_option(_TMUX_PROMPT_TOKEN_OPTION, stored)
        self._prompt_token = stored
        self._prompt_marker = f"__SRW_PROMPT_{stored}__"

    def _install_prompt_marker(
        self,
        tab_name: str,
        *,
        expected_pending: Optional[str] = None,
    ) -> None:
        if self._prompt_marker is None or self._prompt_token is None:
            raise WorkspaceUnavailableError(
                "Remote tmux prompt identity is unavailable"
            )
        self._send_and_wait(
            tab_name,
            f"export PS1={shlex.quote(self._prompt_marker + ' ')}",
            expected_pending=expected_pending,
        )
        self._set_tmux_window_option(
            tab_name,
            _TMUX_WINDOW_PROMPT_OPTION,
            self._prompt_token,
        )
        tab = self._tabs.get(tab_name)
        if tab is None:
            raise WorkspaceUnavailableError(
                "Remote shell transport changed while installing prompt identity"
            )
        tab.prompt_marker_ready = True

    def _discover_single_pane(self, tab_name: str) -> str:
        output = self._tmux_exec_checked(
            f"tmux list-panes -t {self._tmux_target(tab_name)} -F '#{{pane_id}}'",
            operation=f"discover pane for {tab_name}",
        )
        pane_ids = [line.strip() for line in output.splitlines() if line.strip()]
        if len(pane_ids) != 1 or not _TMUX_PANE_ID_PATTERN.fullmatch(pane_ids[0]):
            raise WorkspaceUnavailableError(
                f"Tmux window {tab_name!r} requires exactly one managed pane"
            )
        return pane_ids[0]

    def _rehydrate_tabs(self) -> None:
        """Rebuild local tab guards from authoritative tmux window options."""
        fmt = _TMUX_FIELD_SEPARATOR.join(
            (
                "#{window_name}",
                f"#{{{_TMUX_TAB_TYPE_OPTION}}}",
                f"#{{{_TMUX_PENDING_SENTINEL_OPTION}}}",
                "#{window_panes}",
                "#{pane_id}",
                f"#{{{_TMUX_PANE_ID_OPTION}}}",
                f"#{{{_TMUX_WINDOW_PROMPT_OPTION}}}",
                f"#{{{_TMUX_WINDOW_SETUP_OPTION}}}",
            )
        )
        output = self._tmux_exec_checked(
            f"tmux list-windows -t {self._tmux_target()} -F {shlex.quote(fmt)}",
            operation="list windows for reattach",
        )
        tabs: OrderedDict[str, _RemoteTab] = OrderedDict()
        incomplete_windows: list[str] = []
        option_migrations: list[tuple[str, str, str]] = []
        for raw_line in output.splitlines():
            fields = raw_line.rstrip("\r").split(_TMUX_FIELD_SEPARATOR, 7)
            if len(fields) != 8:
                raise WorkspaceUnavailableError(
                    "Malformed tmux window metadata for session "
                    f"{self._session_name}; refusing partial adoption"
                )
            (
                name,
                stored_type,
                stored_pending,
                pane_count,
                pane_id,
                stored_pane_id,
                stored_prompt_token,
                stored_setup_state,
            ) = fields
            if not TAB_NAME_PATTERN.fullmatch(name):
                raise WorkspaceUnavailableError(
                    f"Tmux window {name!r} cannot be represented safely; "
                    "refusing partial adoption"
                )
            if name in tabs:
                raise WorkspaceUnavailableError(
                    f"Duplicate tmux window name {name!r}; refusing adoption"
                )
            if (
                self._shell_protocol_current
                and stored_setup_state != _TMUX_SETUP_COMPLETE
            ):
                if name == "default":
                    raise WorkspaceUnavailableError(
                        "Remote tmux default window setup is incomplete"
                    )
                incomplete_windows.append(name)
                logger.warning(
                    "Discarding incomplete remote tmux window: session=%s window=%s",
                    self._session_name,
                    name,
                )
                continue
            if pane_count != "1" or not _TMUX_PANE_ID_PATTERN.fullmatch(pane_id):
                raise WorkspaceUnavailableError(
                    f"Tmux window {name!r} has unsupported pane topology "
                    f"(count={pane_count!r}); exactly one managed pane is required"
                )
            if stored_pane_id and stored_pane_id != pane_id:
                raise WorkspaceUnavailableError(
                    f"Tmux window {name!r} pane identity changed; refusing adoption"
                )
            if not stored_pane_id:
                option_migrations.append((name, _TMUX_PANE_ID_OPTION, pane_id))

            # The pre-S2 backend always created `default` as a shell but had no
            # persisted type option. Preserve that rolling-upgrade path. Other
            # missing/future metadata is not assumed to be a synchronous shell:
            # that could type a new command into an inherited REPL/process.
            adopted_legacy_default = not stored_type and name == "default"
            if stored_type in _TMUX_TAB_TYPES:
                tab_type = stored_type
            elif adopted_legacy_default:
                tab_type = "shell"
                logger.info(
                    "Adopting legacy tmux default window as shell: session=%s",
                    self._session_name,
                )
                option_migrations.append((name, _TMUX_TAB_TYPE_OPTION, "shell"))
            else:
                tab_type = "process"
            if tab_type != stored_type and not adopted_legacy_default:
                logger.warning(
                    "Tmux window %s:%s has no supported SRW tab type; "
                    "rehydrating conservatively as process",
                    self._session_name,
                    name,
                )
            tab = _RemoteTab(name=name, tab_type=tab_type, pane_id=pane_id)
            if stored_prompt_token:
                if stored_prompt_token != self._prompt_token:
                    raise WorkspaceUnavailableError(
                        f"Tmux window {name!r} prompt identity changed; "
                        "refusing adoption"
                    )
                tab.prompt_marker_ready = True
            if stored_pending and (
                stored_pending != _INHERITED_BUSY_SENTINEL
                and not _TMUX_PENDING_PATTERN.fullmatch(stored_pending)
            ):
                logger.warning(
                    "Tmux window %s:%s has invalid pending metadata; "
                    "preserving it as an opaque busy pane",
                    self._session_name,
                    name,
                )
                tab.pending_sentinel = _INHERITED_BUSY_SENTINEL
                option_migrations.append(
                    (name, _TMUX_PENDING_SENTINEL_OPTION, _INHERITED_BUSY_SENTINEL)
                )
            else:
                tab.pending_sentinel = stored_pending or None
            tabs[name] = tab

        if not tabs:
            raise WorkspaceUnavailableError(
                f"Remote tmux session {self._session_name} has no usable windows"
            )

        self._tabs = tabs
        for name in incomplete_windows:
            self._tmux_mutate_checked(
                f"tmux kill-window -t {self._tmux_target(name)}",
                operation=f"discard incomplete window {name}",
            )
        for name, option, value in option_migrations:
            self._set_tmux_window_option(name, option, value)
        for name, tab in list(self._tabs.items()):
            if tab.tab_type != "shell":
                continue
            try:
                lines = self._tmux_capture(name)
            except KeyError:
                if name not in self._tabs:
                    # Reattachment consumed the pane-loss error internally so
                    # the claim can continue. Preserve one agent-visible notice
                    # before ensure-tab is allowed to recreate this name.
                    self._lost_tab_notices.add(name)
                    continue
                raise
            except WorkspaceUnavailableError:
                raise
            except Exception:
                # Failure to inspect must never be interpreted as an idle pane.
                lines = []
                if tab.pending_sentinel is None:
                    self._set_tab_pending(
                        name,
                        _INHERITED_BUSY_SENTINEL,
                        expected=None,
                    )
                continue

            pending = tab.pending_sentinel
            pending_finished = bool(
                pending
                and pending != _INHERITED_BUSY_SENTINEL
                and any(
                    _parse_shell_completion_record(line, pending) is not None
                    for line in lines
                )
            )
            if pending and pending_finished:
                self._clear_tab_pending_if_current(name, pending)
            elif pending is None and not self._shell_protocol_current:
                # A pre-protocol shell has no durable evidence that an empty
                # guard means idle. Preserve it as opaque busy state; explicit
                # cancel/reset is the only safe migration.
                self._set_tab_pending(
                    name,
                    _INHERITED_BUSY_SENTINEL,
                    expected=None,
                )

        logger.info(
            "Remote shell reattached: session=%s tabs=%d",
            self._session_name,
            len(self._tabs),
        )

    def _send_and_wait(
        self,
        tab_name: str,
        command: str,
        timeout: float = 5.0,
        *,
        expected_pending: Optional[str] = None,
    ) -> None:
        """Send a setup command and block until it has run (marker prints).

        Mirrors ShellManager._send_and_wait so tab-creation preamble (env, cd)
        settles before the first user command instead of racing it.
        """
        marker = f"__READY_{uuid.uuid4().hex[:8]}__"
        self._reserve_and_send_shell_command(
            tab_name,
            expected=expected_pending,
            sentinel=_INHERITED_BUSY_SENTINEL,
            command=(
                f"{command}; _srw_setup_rc=$?; "
                f'printf \'\\n{marker} %s %s\\n\' "$_srw_setup_rc" "$PWD"'
            ),
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            lines = self._tmux_capture(tab_name)
            ready_record = next(
                (
                    record
                    for ln in reversed(lines)
                    if (record := _parse_shell_completion_record(ln, marker))
                    is not None
                ),
                None,
            )
            if ready_record is not None:
                self._clear_tab_pending(tab_name, _INHERITED_BUSY_SENTINEL)
                setup_rc, _ = ready_record
                if setup_rc != 0:
                    raise WorkspaceUnavailableError(
                        f"Remote tmux setup failed on tab {tab_name!r} "
                        f"with exit code {setup_rc}"
                    )
                return
            time.sleep(0.1)
        raise WorkspaceUnavailableError(
            f"Remote tmux setup did not settle on tab {tab_name!r}"
        )

    def _init_shell(self) -> None:
        """Create or reattach the deterministic remote tmux session."""
        if self._shell_initialized:
            return
        with self._shell_init_lock:
            if self._shell_initialized:
                return
            self._lost_tab_notices.clear()
            started = time.perf_counter()
            self._ensure_connected()
            disposition = self._create_or_observe_tmux_session()
            self._promote_tmux_owner_token()
            setup_state = self._read_tmux_session_option(_TMUX_SETUP_OPTION)
            if setup_state not in {
                "",
                _TMUX_SETUP_PENDING,
                _TMUX_SETUP_COMPLETE,
            }:
                raise WorkspaceUnavailableError(
                    "Remote tmux setup state is malformed; refusing adoption"
                )
            if disposition == "existing" and setup_state == _TMUX_SETUP_PENDING:
                # No shell operation was admitted before setup completed. A
                # retry must not adopt the half-configured pane from $HOME.
                if self._shell_owner_token is not None:
                    self._reset_stateless_tmux_session()
                    disposition = "created"
                else:
                    self._tmux_mutate_checked(
                        f"tmux kill-session -t {self._tmux_target()}",
                        operation="discard incomplete shell setup",
                    )
                    disposition = self._create_or_observe_tmux_session()
                    if disposition != "created":
                        raise WorkspaceUnavailableError(
                            "Could not recreate incomplete remote tmux session"
                        )
                    self._promote_tmux_owner_token()
                setup_state = _TMUX_SETUP_PENDING
            if (
                self._shell_owner_token is not None
                and self.workspace_incarnation_fenced
            ):
                generation = self._read_tmux_session_option(_TMUX_GENERATION_OPTION)
                if not re.fullmatch(r"[0-9a-f]{32}", generation):
                    raise WorkspaceUnavailableError(
                        "Remote tmux process generation is malformed"
                    )
                self._shell_generation = generation
            self._attest_tmux_owner()
            self._ensure_prompt_token()
            stored_protocol = self._read_tmux_session_option(_TMUX_PROTOCOL_OPTION)
            if stored_protocol and stored_protocol != _TMUX_PROTOCOL_VERSION:
                raise WorkspaceUnavailableError(
                    "Remote tmux shell protocol is unsupported; refusing adoption"
                )
            self._shell_protocol_current = stored_protocol == _TMUX_PROTOCOL_VERSION
            if self._shell_protocol_current and setup_state != _TMUX_SETUP_COMPLETE:
                raise WorkspaceUnavailableError(
                    "Remote tmux protocol is current but setup is incomplete"
                )
            self._tmux_mutate_checked(
                f"tmux set-option -t {self._tmux_target()} "
                f"history-limit {self._scrollback_limit}",
                operation="set history limit",
            )

            if disposition == "existing":
                self._rehydrate_tabs()
            else:
                pane_id = self._discover_single_pane("default")
                self._tabs["default"] = _RemoteTab(
                    name="default",
                    tab_type="shell",
                    pane_id=pane_id,
                )
                self._set_tmux_window_option("default", _TMUX_TAB_TYPE_OPTION, "shell")
                self._set_tmux_window_option("default", _TMUX_PANE_ID_OPTION, pane_id)

                # Non-interactive env + working directory; wait for it to
                # settle so the preamble doesn't fold into the first command's
                # output. Only the creator runs it: reattach must preserve cwd,
                # exported variables and foreground/background processes.
                setup = self._with_credential_environment(NONINTERACTIVE_ENV_EXPORT)
                if self.workspace_incarnation_fenced:
                    process_env = self._stateless_process_env_export()
                    setup = f"{process_env}; {setup}"
                if self._sandbox_cwd:
                    setup += f"; cd {self._sandbox_cwd}"
                self._send_and_wait("default", setup)
                self._install_prompt_marker("default")
                self._set_tmux_window_option(
                    "default",
                    _TMUX_WINDOW_SETUP_OPTION,
                    _TMUX_SETUP_COMPLETE,
                )
                self._set_tmux_session_option(
                    _TMUX_SETUP_OPTION,
                    _TMUX_SETUP_COMPLETE,
                )
                logger.info("Remote shell created: session=%s", self._session_name)

            if not self._shell_protocol_current:
                # Stamp only after fresh initialization or conservative legacy
                # rehydration has completed. A crash before this point leaves
                # the next claimant on the fail-closed legacy path.
                if disposition == "existing":
                    for tab_name in self._tabs:
                        self._set_tmux_window_option(
                            tab_name,
                            _TMUX_WINDOW_SETUP_OPTION,
                            _TMUX_SETUP_COMPLETE,
                        )
                    self._set_tmux_session_option(
                        _TMUX_SETUP_OPTION,
                        _TMUX_SETUP_COMPLETE,
                    )
                self._set_tmux_session_option(
                    _TMUX_PROTOCOL_OPTION,
                    _TMUX_PROTOCOL_VERSION,
                )
                self._shell_protocol_current = True

            self._shell_initialized = True
            logger.info(
                "remote shell timing: session=%s disposition=%s tabs=%d total=%.3fs",
                self._session_name,
                disposition,
                len(self._tabs),
                time.perf_counter() - started,
            )

    def _ensure_shell(self) -> None:
        """Ensure shell is initialized before shell operations."""
        if not self._shell_initialized:
            self._init_shell()

    def _tmux_send_keys(self, tab_name: str, text: str, enter: bool = True) -> None:
        """Send keys to a remote tmux pane."""
        pane = self._tmux_pane_target(tab_name)
        tab = self._tabs[tab_name]
        assert tab.pane_id is not None
        if enter:
            command = (
                f"tmux send-keys -t {pane} -l {shlex.quote(text)}\n"
                f"tmux send-keys -t {pane} Enter"
            )
        else:
            # Raw-key mode intentionally interprets tmux key names such as C-c.
            command = f"tmux send-keys -t {pane} {shlex.quote(text)}"
        self._tmux_mutate_checked(
            command,
            operation=f"send keys to {tab_name}",
            pane_context=(tab_name, tab.pane_id),
        )

    def _tmux_capture(self, tab_name: str) -> List[str]:
        """Capture logical pane lines from remote tmux.

        ``-J`` joins display-width wraps while preserving real line breaks.
        Completion records contain an absolute cwd and can exceed the pane
        width; without join-lines, strict parsing would leave the durable
        pending guard stuck even though the shell had completed.
        """
        pane = self._tmux_pane_target(tab_name)
        tab = self._tabs[tab_name]
        assert tab.pane_id is not None
        output = self._tmux_exec_checked(
            f"tmux capture-pane -J -t {pane} -p -S -{self._scrollback_limit}",
            operation=f"capture pane {tab_name}",
            pane_context=(tab_name, tab.pane_id),
        )
        lines = output.splitlines()
        # Strip trailing empty lines
        while lines and not lines[-1].strip():
            lines.pop()
        return lines

    def _check_blocked(self, command: str) -> Optional[str]:
        """Return error message if command is blocked, else None.

        Returns SUDO_FREEZE_SENTINEL when sudo_action is "freeze" and the
        command starts with sudo. The tool layer detects this sentinel and
        triggers a job freeze (VM upgrade prompt).
        """
        first_word = command.strip().split()[0] if command.strip() else ""
        if not first_word:
            return None

        # Sudo intercept (separate from blocked_commands)
        if first_word == "sudo":
            if self._sudo_action == "allow":
                return None  # VM-backed workspace: pass through to sudo gate
            elif self._sudo_action == "freeze":
                return SUDO_FREEZE_SENTINEL
            else:  # "block"
                return self._sudo_block_message or (
                    "Command blocked: 'sudo' is not available in this workspace. "
                    "System package installation requires a VM runtime."
                )

        # Standard blocked commands
        if self._blocked_commands and first_word in self._blocked_commands:
            return (
                f"Command blocked: '{first_word}' is not allowed. "
                f"Blocked commands: {', '.join(sorted(self._blocked_commands))}"
            )
        return None

    def _detect_interactive_prompt(self, all_lines: List[str]) -> Optional[str]:
        """Check if the terminal appears to be waiting for interactive input."""
        check_lines = all_lines[-5:] if len(all_lines) >= 5 else all_lines
        text_to_check = "\n".join(check_lines)

        for pattern, description in INTERACTIVE_PROMPT_PATTERNS:
            if pattern.search(text_to_check):
                return description

        return None

    def _detect_blocked_tab(self, tab_name: str, all_lines: List[str]) -> Optional[str]:
        """Pre-flight check: is the tab stuck on a prompt right now?"""
        check_lines = all_lines[-3:] if len(all_lines) >= 3 else all_lines
        text_to_check = "\n".join(check_lines)

        for pattern, description in INTERACTIVE_PROMPT_PATTERNS:
            if pattern.search(text_to_check):
                return description

        return None

    def _shell_tab_lock(self, tab_name: str) -> Any:
        """Return the process-local serialization lock for one tmux pane."""
        with self._tab_sync_locks_guard:
            lock = self._tab_sync_locks.get(tab_name)
            if lock is None:
                # Tab creation calls back through ``shell_open_tab`` while the
                # ensure path owns this lock.  Keep it re-entrant so the
                # absence check and the complete window setup are one local
                # critical section without deadlocking that public wrapper.
                lock = threading.RLock()
                self._tab_sync_locks[tab_name] = lock
            return lock

    def _build_guarded_shell_command(
        self,
        command: str,
        sentinel: str,
        working_dir: Optional[str],
    ) -> Tuple[str, Optional[str]]:
        """Build one command whose sentinel follows any requested cwd restore."""
        command = self._with_credential_environment(command)
        if not working_dir:
            return build_sentinel_command(command, sentinel)

        full_dir = posixpath.normpath(
            posixpath.join(self._sandbox_cwd, working_dir)
            if self._sandbox_cwd
            else working_dir
        )
        if "\n" in command:
            outer_delim = f"SRW_DELIM_{uuid.uuid4().hex[:12]}"
            start_marker = f"__SRW_START_{uuid.uuid4().hex[:12]}__"
            user_command = (
                f'bash << "{outer_delim}"\n'
                f'echo "{start_marker}"\n'
                f"{command}\n"
                f"{outer_delim}"
            )
        else:
            start_marker = None
            user_command = command
        root_dir = self._sandbox_cwd or self._remote_root
        full_command = (
            f"cd {shlex.quote(full_dir)} && {user_command}\n"
            '_srw_rc=$?; _srw_cwd="$PWD"; '
            f"cd {shlex.quote(root_dir)} && "
            f'printf \'\\n{sentinel} %s %s\\n\' "$_srw_rc" "$_srw_cwd"'
        )
        return full_command, start_marker

    def shell_run(
        self,
        command: str,
        timeout: Optional[int] = None,
        tab_name: str = "default",
        working_dir: Optional[str] = None,
    ) -> str:
        """Execute a command synchronously with sentinel-based completion detection.

        Mirrors the logic of ShellManager.run_sync() but over SSH+remote tmux.
        """
        self._ensure_shell()

        blocked = self._check_blocked(command)
        if blocked:
            return blocked

        # Explicit timeout disables the soft no-change timeout; only the hard
        # timeout bounds the command then.
        soft_enabled = timeout is None
        if timeout is None:
            timeout = self._default_timeout
        timeout = min(timeout, HARD_TIMEOUT_CAP_SECONDS)
        sentinel = f"__DONE_{uuid.uuid4().hex[:12]}__"

        if tab_name not in self._tabs:
            self.shell_ensure_tab(tab_name)
        tab = self._tabs[tab_name]

        if tab.tab_type not in ("shell",):
            raise ValueError(
                f"Synchronous execution only works on shell-type tabs. "
                f"Tab '{tab_name}' is type '{tab.tab_type}'. "
                f"Use shell_send for interactive tabs."
            )

        with self._shell_tab_lock(tab_name):
            # Pre-flight check
            pre_lines = self._tmux_capture(tab_name)
            # The durable guard, not scrollback that merely resembles a
            # prompt, decides whether there can be a live foreground command.
            # First consume an exact completion record. Only an outstanding
            # guard may then turn a current prompt into a blocked-tab result.
            if tab.pending_sentinel is not None:
                prev_done = bool(
                    tab.pending_sentinel != _INHERITED_BUSY_SENTINEL
                    and any(
                        _parse_shell_completion_record(
                            ln,
                            tab.pending_sentinel,
                        )
                        is not None
                        for ln in pre_lines
                    )
                )
                if prev_done:
                    self._clear_tab_pending_if_current(
                        tab_name,
                        tab.pending_sentinel,
                    )
                else:
                    existing_prompt = self._detect_blocked_tab(tab_name, pre_lines)
                    if existing_prompt:
                        state_lines = [ln for ln in pre_lines if ln.strip()][-30:]
                        terminal_state = "\n".join(state_lines) or "(empty)"
                        return (
                            f"Tab '{tab_name}' is blocked by a previous "
                            f"{existing_prompt}. Your command was NOT executed.\n"
                            f"Resolve the prompt first: send the expected input "
                            f"with keys mode (e.g. keys='N' or keys='yes'), "
                            f"send C-c to cancel, or use a different tab.\n"
                            f"--- terminal state ---\n{terminal_state}"
                        )
                    state_lines = [ln for ln in pre_lines if ln.strip()][-30:]
                    terminal_state = "\n".join(state_lines) or "(empty)"
                    return COLLIDING_COMMAND_TEMPLATE.format(
                        tab=tab_name, terminal_state=terminal_state
                    )

            pre_count = len(pre_lines)

            # Build the sentinel-suffixed command. When working_dir is set the
            # cwd enter, user command, root restore and sentinel stay inside
            # this one remotely reserved send.
            full_cmd, start_marker = self._build_guarded_shell_command(
                command,
                sentinel,
                working_dir,
            )
            # Persist ownership before typing the command. If this agent dies
            # immediately afterward, the next claimant blocks a colliding
            # synchronous command until it observes the sentinel or a prompt.
            self._reserve_and_send_shell_command(
                tab_name,
                expected=None,
                sentinel=sentinel,
                command=full_cmd,
            )

            # Poll for sentinel
            start_time = time.monotonic()
            output_text = ""
            exit_code = None
            resolved_cwd = None
            last_content_hash = None
            stall_start = None

            while time.monotonic() - start_time < timeout:
                time.sleep(0.3)  # Slightly longer poll interval for SSH latency
                try:
                    all_lines = self._tmux_capture(tab_name)
                except WorkspaceUnavailableError:
                    raise

                # Find sentinel output line
                sentinel_line_idx = None
                completion_record: Optional[Tuple[int, str]] = None
                for i in range(len(all_lines) - 1, -1, -1):
                    parsed = _parse_shell_completion_record(all_lines[i], sentinel)
                    if parsed is not None:
                        sentinel_line_idx = i
                        completion_record = parsed
                        break

                if sentinel_line_idx is not None and completion_record is not None:
                    exit_code, resolved_cwd = completion_record

                    # Command finished — tab is no longer busy.
                    self._clear_tab_pending_if_current(tab_name, sentinel)

                    if start_marker is not None:
                        # Multi-line wrap path: locate the start marker output
                        # line and extract everything between it and the sentinel.
                        start_idx = None
                        for i in range(sentinel_line_idx - 1, -1, -1):
                            if all_lines[i].strip() == start_marker:
                                start_idx = i
                                break
                        if start_idx is not None:
                            new_lines = all_lines[start_idx + 1 : sentinel_line_idx]
                            output_lines = [
                                ol
                                for ol in new_lines
                                if start_marker not in ol and sentinel not in ol
                            ]
                        else:
                            new_lines = all_lines[pre_count:sentinel_line_idx]
                            output_lines = [
                                ol for ol in new_lines if sentinel not in ol
                            ]
                    else:
                        new_lines = all_lines[pre_count:sentinel_line_idx]
                        output_lines = [ol for ol in new_lines if sentinel not in ol]
                        # Skip prompt/command echo lines
                        while output_lines and (
                            command.split()[0] in output_lines[0]
                            or output_lines[0].strip().endswith("$")
                        ):
                            output_lines = output_lines[1:]

                    output_text = "\n".join(output_lines).strip()
                    break

                # Genuine interactive prompt (waiting for input)
                elapsed = time.monotonic() - start_time
                if elapsed > 1.0:
                    prompt_type = self._detect_interactive_prompt(all_lines)
                    if prompt_type:
                        terminal_state = self._capture_terminal_state(
                            tab_name, sentinel, pre_count
                        )
                        # Command owns the pane; don't cwd-restore into it.
                        tab.pending_sentinel = sentinel
                        tab.last_activity = datetime.now(timezone.utc)
                        return INTERACTIVE_PROMPT_TEMPLATE.format(
                            prompt_type=prompt_type,
                            tab=tab_name,
                            terminal_state=terminal_state,
                        )

                    # Soft no-change timeout (command still running)
                    last_content_hash, stall_start, timed_out = compute_no_change_state(
                        all_lines,
                        last_content_hash,
                        stall_start,
                        time.monotonic(),
                        soft_enabled,
                        self._no_change_timeout,
                    )
                    if timed_out:
                        terminal_state = self._capture_terminal_state(
                            tab_name, sentinel, pre_count
                        )
                        # Leave it running; don't cwd-restore into a busy pane.
                        tab.pending_sentinel = sentinel
                        tab.last_activity = datetime.now(timezone.utc)
                        return STILL_RUNNING_TEMPLATE.format(
                            tab=tab_name,
                            elapsed=elapsed,
                            quiet=time.monotonic() - stall_start,
                            terminal_state=terminal_state,
                        )

            # Hard timeout: loop exited without the sentinel -> still running.
            if exit_code is None:
                terminal_state = self._capture_terminal_state(
                    tab_name, sentinel, pre_count
                )
                tab.pending_sentinel = sentinel
                tab.last_activity = datetime.now(timezone.utc)
                return STILL_RUNNING_HARDCAP_TEMPLATE.format(
                    tab=tab_name, elapsed=timeout, terminal_state=terminal_state
                )

            tab.last_activity = datetime.now(timezone.utc)

            # Format output
            parts = [
                f"Exit code: {exit_code}",
                f"CWD: {resolved_cwd or '(unknown)'}",
            ]
            if output_text:
                parts.append(f"--- stdout ---\n{output_text}")
            else:
                parts.append("(no output)")

            return "\n".join(parts)

    def _capture_terminal_state(
        self, tab_name: str, sentinel: str, pre_count: int
    ) -> str:
        """Capture terminal state for timeout/stall reporting."""
        try:
            all_lines = self._tmux_capture(tab_name)
            post_lines = all_lines[pre_count:]
            clean_lines = [line for line in post_lines if sentinel not in line]
            if not clean_lines:
                clean_lines = all_lines[-30:]
            if len(clean_lines) > 30:
                clean_lines = clean_lines[-30:]
            while clean_lines and not clean_lines[-1].strip():
                clean_lines.pop()
            return "\n".join(clean_lines)
        except KeyError:
            raise
        except Exception as e:
            logger.debug(f"Failed to capture terminal state: {e}")
            return "(failed to capture terminal state)"

    def shell_send(
        self,
        tab_name: str,
        text: str,
        enter: bool = True,
        working_dir: Optional[str] = None,
        allow_busy: bool = False,
    ) -> str:
        self._ensure_shell()

        if working_dir and not enter:
            raise ValueError("working_dir cannot be applied to raw keystrokes")

        if enter:
            blocked = self._check_blocked(text)
            if blocked:
                return blocked

        if tab_name not in self._tabs:
            raise KeyError(
                f"Tab '{tab_name}' not found. Available: {', '.join(self._tabs.keys())}"
            )

        with self._shell_tab_lock(tab_name):
            tab = self._tabs[tab_name]
            if (
                enter
                and tab.tab_type == "shell"
                and tab.pending_sentinel is not None
                and not allow_busy
            ):
                lines = self._tmux_capture(tab_name)
                pending = tab.pending_sentinel
                pending_finished = bool(
                    pending != _INHERITED_BUSY_SENTINEL
                    and any(
                        _parse_shell_completion_record(line, pending) is not None
                        for line in lines
                    )
                )
                if pending_finished:
                    self._clear_tab_pending_if_current(tab_name, pending)
                else:
                    terminal_state = (
                        "\n".join([line for line in lines if line.strip()][-30:])
                        or "(empty)"
                    )
                    return COLLIDING_COMMAND_TEMPLATE.format(
                        tab=tab_name,
                        terminal_state=terminal_state,
                    )
            if enter and tab.tab_type == "shell" and tab.pending_sentinel is None:
                # Even explicit keys-mode text is a normal command when the
                # v2 durable guard says the shell is idle. Only a response sent
                # while an existing guard is present is raw foreground input.
                sentinel = f"__DONE_{uuid.uuid4().hex[:12]}__"
                guarded_command, _ = self._build_guarded_shell_command(
                    text,
                    sentinel,
                    working_dir,
                )
                self._reserve_and_send_shell_command(
                    tab_name,
                    expected=None,
                    sentinel=sentinel,
                    command=guarded_command,
                )
            else:
                if working_dir:
                    raise ValueError(
                        "working_dir requires an idle shell command target"
                    )
                self._tmux_send_keys(tab_name, text, enter=enter)
            tab.last_activity = datetime.now(timezone.utc)
        return f"Sent to '{tab_name}'"

    def shell_cancel(self, tab_name: str = "default") -> str:
        """Send Ctrl+C to a shell tab to abort a stuck/hung command.

        The completion probe, not prompt-looking terminal output, proves that
        Bash accepted the interrupt and is reading commands again. A process
        that catches/ignores SIGINT cannot forge that transition accidentally;
        after two failed probes the explicit cancel resets the pane.
        """
        self._ensure_shell()
        if tab_name not in self._tabs:
            raise KeyError(
                f"Tab '{tab_name}' not found. Available: {', '.join(self._tabs.keys())}"
            )
        tab = self._tabs[tab_name]

        with self._shell_tab_lock(tab_name):
            if tab.pending_sentinel is None:
                return f"Tab '{tab_name}': nothing to cancel (no pending command)."

            # Up to two Ctrl+C + probe attempts (some programs need a second).
            for _ in range(2):
                expected = tab.pending_sentinel
                sentinel = f"__DONE_{uuid.uuid4().hex[:12]}__"
                self._cancel_and_probe_shell_command(
                    tab_name,
                    expected=expected,
                    sentinel=sentinel,
                )
                time.sleep(_CANCEL_SETTLE_SECONDS)
                lines = self._tmux_capture(tab_name)
                if any(
                    _parse_shell_completion_record(line, sentinel) is not None
                    for line in lines
                ):
                    self._clear_tab_pending_if_current(tab_name, sentinel)
                    tab.last_activity = datetime.now(timezone.utc)
                    return (
                        f"Sent Ctrl+C to tab '{tab_name}'; the command was "
                        f"interrupted and the tab is free."
                    )

        # Still stuck (process ignoring SIGINT). Reset the tab outside the lock:
        # close + reopen yields a fresh _RemoteTab (pending_sentinel cleared by
        # construction) and leaves the tab immediately usable.
        if len(self._tabs) == 1 and self._shell_owner_token is not None:
            self._reset_stateless_tmux_session()
            self._init_shell()
        else:
            self.shell_close_tab(tab_name)
            self.shell_ensure_tab(tab_name)
        return (
            f"Tab '{tab_name}' did not respond to Ctrl+C; the shell tab was "
            f"reset (shell-local state and background jobs were cleared)."
        )

    def shell_read(
        self,
        tab_name: str,
        lines: int = 50,
        since_cursor: bool = False,
    ) -> Tuple[str, Dict[str, Any]]:
        self._ensure_shell()

        if tab_name not in self._tabs:
            raise KeyError(
                f"Tab '{tab_name}' not found. Available: {', '.join(self._tabs.keys())}"
            )

        tab = self._tabs[tab_name]
        all_lines = self._tmux_capture(tab_name)
        total_lines = len(all_lines)

        if since_cursor:
            new_lines = all_lines[tab.read_cursor :]
            tab.read_cursor = total_lines
            text = "\n".join(new_lines) if new_lines else "(no new output)"
            metadata = {
                "tab": tab_name,
                "mode": "since_cursor",
                "lines_returned": len(new_lines),
                "total_lines": total_lines,
            }
        else:
            start = max(0, total_lines - lines)
            selected = all_lines[start:]
            tab.read_cursor = total_lines
            text = "\n".join(selected) if selected else "(empty)"
            metadata = {
                "tab": tab_name,
                "mode": "tail",
                "lines_requested": lines,
                "lines_returned": len(selected),
                "total_lines": total_lines,
            }

        return text, metadata

    def shell_read_with_offset(
        self,
        tab_name: str,
        lines: int = 30,
        offset: Optional[int] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        self._ensure_shell()

        if tab_name not in self._tabs:
            raise KeyError(
                f"Tab '{tab_name}' not found. Available: {', '.join(self._tabs.keys())}"
            )

        all_lines = self._tmux_capture(tab_name)
        total_lines = len(all_lines)

        if offset is not None:
            start = max(0, min(offset, total_lines))
            end = min(start + lines, total_lines)
            selected = all_lines[start:end]
            text = "\n".join(selected) if selected else "(empty)"
            metadata = {
                "tab": tab_name,
                "mode": "offset",
                "offset": start,
                "lines_returned": len(selected),
                "total_lines": total_lines,
            }
        else:
            start = max(0, total_lines - lines)
            selected = all_lines[start:]
            text = "\n".join(selected) if selected else "(empty)"
            metadata = {
                "tab": tab_name,
                "mode": "tail",
                "lines_returned": len(selected),
                "total_lines": total_lines,
            }

        return text, metadata

    def shell_ensure_tab(self, name: str) -> None:
        self._ensure_shell()
        # Tool calls from one model response execute concurrently.  Several
        # shell_execute calls may therefore all target the same new name.  The
        # existence check must share the pane lock with creation; otherwise
        # each caller can create a duplicate tmux window before ``_tabs`` is
        # populated and exact-name discovery becomes ambiguous.
        with self._shell_tab_lock(name):
            if name in self._tabs:
                return
            if name in self._lost_tab_notices:
                self._lost_tab_notices.remove(name)
                raise KeyError(self._lost_tab_message(name))
            self.shell_open_tab(name)

    def shell_open_tab(
        self,
        name: str,
        command: Optional[str] = None,
        tab_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_shell()
        with self._shell_tab_lock(name):
            return self._shell_open_tab_locked(
                name,
                command=command,
                tab_type=tab_type,
            )

    def _shell_open_tab_locked(
        self,
        name: str,
        command: Optional[str] = None,
        tab_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_shell()

        if not TAB_NAME_PATTERN.match(name):
            raise ValueError(
                f"Invalid tab name '{name}': must match {TAB_NAME_PATTERN.pattern}"
            )

        if name in self._tabs:
            raise ValueError(f"Tab '{name}' already exists")

        if len(self._tabs) >= self._max_tabs:
            tab_names = ", ".join(self._tabs.keys())
            raise ValueError(
                f"Maximum tabs ({self._max_tabs}) reached. Close unused tabs first. "
                f"Open tabs: {tab_names}"
            )

        # Auto-detect type
        if tab_type is None:
            tab_type = "shell"
            if command:
                from shared.runtime.core.shell_protocol import COMMAND_TYPE_MAP

                first_word = command.strip().split()[0]
                base_cmd = first_word.rsplit("/", 1)[-1]
                tab_type = COMMAND_TYPE_MAP.get(base_cmd, "process")
        if tab_type not in _TMUX_TAB_TYPES:
            raise ValueError(
                f"Invalid tab type {tab_type!r}: expected one of "
                f"{', '.join(sorted(_TMUX_TAB_TYPES))}"
            )

        # Create and attest one managed pane before any setup keystrokes.
        self._tmux_mutate_checked(
            f"tmux new-window -t {self._tmux_target()} -n {shlex.quote(name)} -d "
            f"\\; set-option -w -t {self._tmux_target(name)} "
            f"{_TMUX_WINDOW_SETUP_OPTION} {_TMUX_SETUP_PENDING}",
            operation=f"create window {name}",
        )
        try:
            pane_id = self._discover_single_pane(name)
            tab = _RemoteTab(name=name, tab_type=tab_type, pane_id=pane_id)
            self._tabs[name] = tab
            self._lost_tab_notices.discard(name)
            self._set_tmux_window_option(name, _TMUX_TAB_TYPE_OPTION, tab_type)
            self._set_tmux_window_option(name, _TMUX_PANE_ID_OPTION, pane_id)

            # Non-interactive env + working directory (shell/process tabs only).
            if tab_type in ("shell", "process"):
                setup = self._with_credential_environment(NONINTERACTIVE_ENV_EXPORT)
                if self.workspace_incarnation_fenced:
                    process_env = self._stateless_process_env_export()
                    setup = f"{process_env}; {setup}"
                if self._sandbox_cwd:
                    setup += f"; cd {self._sandbox_cwd}"
                self._send_and_wait(name, setup)
                if tab_type == "shell":
                    self._install_prompt_marker(name)

            # ssh/repl tabs have no SRW preamble. For every type, persisted
            # type/pane metadata plus any applicable preamble is complete before
            # an initial foreground command is admitted.
            self._set_tmux_window_option(
                name,
                _TMUX_WINDOW_SETUP_OPTION,
                _TMUX_SETUP_COMPLETE,
            )
        except Exception:
            self._tabs.pop(name, None)
            try:
                self._tmux_mutate_checked(
                    f"tmux kill-window -t {self._tmux_target(name)}",
                    operation=f"discard failed window setup for {name}",
                )
            except Exception:
                logger.warning(
                    "Could not discard failed remote tmux window setup: %s",
                    name,
                    exc_info=True,
                )
            raise

        # Run initial command
        if command:
            if tab_type == "shell":
                sentinel = f"__DONE_{uuid.uuid4().hex[:12]}__"
                guarded_command, _ = self._build_guarded_shell_command(
                    command,
                    sentinel,
                    None,
                )
                self._reserve_and_send_shell_command(
                    name,
                    expected=None,
                    sentinel=sentinel,
                    command=guarded_command,
                )
            else:
                self._reserve_and_send_shell_command(
                    name,
                    expected=None,
                    sentinel=_INHERITED_BUSY_SENTINEL,
                    command=self._with_credential_environment(command),
                )

        logger.info(f"Opened remote tab '{name}' (type={tab_type})")
        return tab.to_metadata()

    def shell_close_tab(self, name: str) -> str:
        self._ensure_shell()

        if name not in self._tabs:
            raise KeyError(
                f"Tab '{name}' not found. Available: {', '.join(self._tabs.keys())}"
            )

        was_last_window = len(self._tabs) == 1
        if was_last_window and self._shell_owner_token is not None:
            raise ValueError(
                "The final stateless shell tab cannot be closed; use cancel to "
                "reset a stuck shell while preserving the ownership tombstone"
            )
        self._tmux_mutate_checked(
            f"tmux kill-window -t {self._tmux_target(name)}",
            operation=f"kill window {name}",
        )
        del self._tabs[name]
        if was_last_window:
            # tmux destroys a session with its final window. Force the normal
            # create/attest path on the next ensure (shell_cancel's reset path
            # immediately does so).
            self._shell_initialized = False
            self._prompt_token = None
            self._prompt_marker = None
        logger.info(f"Closed remote tab '{name}'")
        return f"Tab '{name}' closed"

    def shell_list_tabs(self) -> List[Dict[str, Any]]:
        self._ensure_shell()
        return [tab.to_metadata() for tab in self._tabs.values()]

    def shell_format_tab_header(self) -> str:
        self._ensure_shell()
        names = list(self._tabs.keys())
        return f"[Shells: {' | '.join(names)}]"

    def shell_cleanup(self) -> None:
        """Explicitly destroy the durable remote shell session.

        This is intentionally separate from :meth:`disconnect`. It is called
        only for a genuine session end or when the backing workspace is being
        retired during a tier swap.
        """
        try:
            if self._shell_owner_token is not None:
                token = str(self._shell_owner_token)
                target = self._tmux_target()
                expected_owner = self._job_id or self._session_name
                fallback_generation = uuid.uuid4().hex
                if not self.workspace_incarnation_fenced:
                    load_for_retirement = (
                        "if _srw_load_state; then\n"
                        f'  [ "$_srw_token" -le {shlex.quote(token)} ] || exit 75\n'
                        "else\n"
                        '  _srw_rc=$?; [ "$_srw_rc" -eq 1 ] || exit "$_srw_rc"\n'
                        f"  _srw_generation={shlex.quote(fallback_generation)}\n"
                        "fi\n"
                    )
                    tmux_incarnation_check = ""
                else:
                    assert self._workspace_generation is not None
                    assert self._runtime_incarnation is not None
                    expected_workspace = shlex.quote(self._workspace_generation)
                    expected_runtime = shlex.quote(self._runtime_incarnation)
                    if self._shared_workspace:
                        # Eager claim installs the exact child marker before
                        # graph work. Without it there is no safe shell
                        # generation with which to find disowned child
                        # processes; a random fallback would falsely prove
                        # zero and allow queue completion.
                        missing_marker_retirement = "  exit 79\n"
                    else:
                        missing_marker_retirement = (
                            "  _srw_retire_incarnation=absent\n"
                            f"  _srw_generation={shlex.quote(fallback_generation)}\n"
                        )
                    load_for_retirement = (
                        "if _srw_load_state; then\n"
                        '  [ "$_srw_process_tagged" = true ] || exit 81\n'
                        '  if [ -z "$_srw_workspace_generation" ] && '
                        '[ -z "$_srw_runtime_incarnation" ]; then\n'
                        "    _srw_retire_incarnation=legacy\n"
                        f'    [ "$_srw_token" -le {shlex.quote(token)} ] '
                        "|| exit 75\n"
                        f'  elif [ "$_srw_workspace_generation" = '
                        f"{expected_workspace} ] && "
                        f'[ "$_srw_runtime_incarnation" = {expected_runtime} ]; then\n'
                        "    _srw_retire_incarnation=current\n"
                        f'    [ "$_srw_token" -le {shlex.quote(token)} ] '
                        "|| exit 75\n"
                        "  else\n"
                        "    _srw_retire_incarnation=stale\n"
                        f'    [ "$_srw_token" -le {shlex.quote(token)} ] '
                        "|| exit 75\n"
                        f"    tmux has-session -t {target} 2>/dev/null && exit 80\n"
                        f"    _srw_generation={shlex.quote(fallback_generation)}\n"
                        "  fi\n"
                        "else\n"
                        '  _srw_rc=$?; [ "$_srw_rc" -eq 1 ] || exit "$_srw_rc"\n'
                        + missing_marker_retirement
                        + "fi\n"
                    )
                    tmux_incarnation_check = (
                        "  _srw_tmux_workspace_generation=$(tmux display-message -p "
                        f"-t {target} '#{{{_TMUX_WORKSPACE_GENERATION_OPTION}}}')\n"
                        "  _srw_tmux_runtime_incarnation=$(tmux display-message -p "
                        f"-t {target} '#{{{_TMUX_RUNTIME_INCARNATION_OPTION}}}')\n"
                        '  if [ "$_srw_retire_incarnation" = current ]; then\n'
                        f'    [ "$_srw_tmux_workspace_generation" = '
                        f"{expected_workspace} ] || exit 80\n"
                        f'    [ "$_srw_tmux_runtime_incarnation" = '
                        f"{expected_runtime} ] || exit 80\n"
                        "  else\n"
                        '    [ -z "$_srw_tmux_workspace_generation" ] || '
                        f'[ "$_srw_tmux_workspace_generation" = '
                        f"{expected_workspace} ] || exit 80\n"
                        '    [ -z "$_srw_tmux_runtime_incarnation" ] || '
                        f'[ "$_srw_tmux_runtime_incarnation" = '
                        f"{expected_runtime} ] || exit 80\n"
                        "  fi\n"
                        + self._stateless_tmux_process_tag_check_shell(
                            target,
                            indent="  ",
                        )
                    )
                inner = (
                    self._tmux_state_shell()
                    + load_for_retirement
                    + f"if tmux has-session -t {target} 2>/dev/null; then\n"
                    + "  _srw_tmux_owner=$(tmux display-message -p "
                    + f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
                    + '  [ -z "$_srw_tmux_owner" ] || '
                    + f'[ "$_srw_tmux_owner" = {shlex.quote(expected_owner)} ] '
                    + "|| exit 73\n"
                    + "  _srw_tmux_token=$(tmux display-message -p "
                    + f"-t {target} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
                    + '  case "$_srw_tmux_token" in "" ) _srw_tmux_token=0 ;; '
                    + "*[!0-9]* ) exit 76 ;; esac\n"
                    + f'  [ "$_srw_tmux_token" -le {shlex.quote(token)} ] '
                    + "|| exit 75\n"
                    + "  _srw_tmux_generation=$(tmux display-message -p "
                    + f"-t {target} '#{{{_TMUX_GENERATION_OPTION}}}')\n"
                    + '  if [ -n "$_srw_tmux_generation" ]; then\n'
                    + '    printf "%s" "$_srw_tmux_generation" | '
                    + "grep -Eq '^[0-9a-f]{32}$' || exit 79\n"
                    + '    if [ -n "$_srw_status" ]; then '
                    + '[ "$_srw_tmux_generation" = "$_srw_generation" ] '
                    + "|| exit 79; fi\n"
                    + "    _srw_generation=$_srw_tmux_generation\n"
                    + "  fi\n"
                    + tmux_incarnation_check
                    + f"  _srw_write_state retired {shlex.quote(token)} "
                    + '"$_srw_generation"\n'
                    + f"  tmux kill-session -t {target} || exit 77\n"
                    + "else\n"
                    + f"  _srw_write_state retired {shlex.quote(token)} "
                    + '"$_srw_generation"\n'
                    + "fi\n"
                    + (
                        self._stateless_terminal_process_zero_shell(terminate=True)
                        if self.workspace_incarnation_fenced
                        else ""
                    )
                )
                self._tmux_exec_checked(
                    self._tmux_lock_command(inner),
                    operation="retire stateless shell session",
                    allow_shell_retired=True,
                    close_sftp=True,
                )
                self._tabs.clear()
                self._shell_initialized = False
                self._shell_generation = None
                if self.workspace_incarnation_fenced:
                    logger.info(
                        "Remote shell retirement acknowledged: session=%s "
                        "workspace_generation=%s runtime_incarnation=%s",
                        self._session_name,
                        self._workspace_generation,
                        self._runtime_incarnation,
                    )
                return

            # A genuine end may inherit a shell without ever opening it locally.
            # Under one workspace lock: refuse a full-id mismatch, monotonically
            # adopt the current claim token (if any), then destroy. This does
            # not create a missing session and is allowed after local shell
            # retirement because cleanup itself is the terminal owner action.
            expected_owner = self._job_id or self._session_name
            token_check = ""
            if self._shell_owner_token is not None:
                token = str(self._shell_owner_token)
                token_check = (
                    "_srw_token=$(tmux display-message -p "
                    f"-t {self._tmux_target()} '#{{{_TMUX_OWNER_TOKEN_OPTION}}}')\n"
                    "case \"$_srw_token\" in '' ) _srw_token=0 ;; "
                    "*[!0-9]* ) exit 76 ;; esac\n"
                    f'[ "$_srw_token" -le {shlex.quote(token)} ] || exit 75\n'
                    f"tmux set-option -t {self._tmux_target()} "
                    f"{_TMUX_OWNER_TOKEN_OPTION} {shlex.quote(token)}\n"
                )
            strict_workspace_process_fence = bool(
                getattr(self, "_strict_terminal_cleanup", False)
                and self.workspace_incarnation_fenced
                and self._shell_owner_token is None
                and not self._shared_workspace
                and self._sudo_action == "freeze"
                and self._workspace_owner_kind == "session"
                and bool(self._workspace_owner_id)
                and bool(self._expected_host_key_fingerprint)
                and self._workspace_tier == "sandbox"
            )
            process_environment_check = ""
            process_zero = ""
            if strict_workspace_process_fence:
                expected_workspace_env = shlex.quote(
                    _WORKSPACE_PROCESS_TAG_ENV + "=" + self._workspace_process_tag()
                )
                process_environment_check = (
                    "_srw_workspace_env=$(tmux show-environment -t "
                    f"{self._tmux_target()} {_WORKSPACE_PROCESS_TAG_ENV} "
                    "2>/dev/null || true)\n"
                    f'[ "$_srw_workspace_env" = {expected_workspace_env} ] '
                    "|| exit 81\n"
                )
                process_zero = self._dedicated_workspace_uid_process_zero_shell(
                    terminate=True
                )
            inner = (
                self._pinned_tmux_fence_shell()
                + f"if tmux has-session -t {self._tmux_target()} 2>/dev/null; then\n"
                "  _srw_id=$(tmux display-message -p "
                f"-t {self._tmux_target()} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
                f'  [ -z "$_srw_id" ] || '
                f'[ "$_srw_id" = {shlex.quote(expected_owner)} ] || exit 73\n'
                + "".join(
                    f"  {line}\n" for line in process_environment_check.splitlines()
                )
                + "".join(f"  {line}\n" for line in token_check.splitlines())
                + f"  tmux kill-session -t {self._tmux_target()} || exit 77\n"
                + "fi\n"
                + process_zero
            )
            self._tmux_exec_checked(
                self._tmux_lock_command(inner),
                operation=(
                    "retire protected shell and workspace processes"
                    if strict_workspace_process_fence
                    else "kill session"
                ),
                allow_shell_retired=True,
            )
            if strict_workspace_process_fence:
                self._terminal_local_quiescence_protocol = "workspace_process_zero_v1"
        except Exception as e:
            logger.warning(f"Error cleaning up remote tmux session: {e}")
            if getattr(self, "_strict_terminal_cleanup", False) or (
                self._shell_owner_token is not None
                and self.workspace_incarnation_fenced
            ):
                self._tabs.clear()
                self._shell_initialized = False
                self._shell_generation = None
                raise WorkspaceUnavailableError(
                    "Remote shell terminal retirement was not acknowledged"
                ) from e
        self._tabs.clear()
        self._shell_initialized = False
        self._shell_generation = None

    def shell_cleanup_strict(self) -> None:
        """Destroy a pinned shell and require a remote acknowledgement."""

        self._strict_terminal_cleanup = True
        try:
            self.shell_cleanup()
        finally:
            self._strict_terminal_cleanup = False

    def protected_workspace_zero_cleanup_strict(self) -> str:
        """Retire and prove zero writers for one exact protected sandbox.

        This narrow primitive is reusable by both the live agent and an
        orchestrator reconciler after the agent Pod is proven absent. Callers
        must construct this backend from the captured workspace endpoint,
        generation, runtime incarnation and pinned SSH host identity. It
        refuses shared/stateless/VM-shaped authority and returns only the
        finite proof protocol accepted by retirement settlement.
        """

        if (
            self._shell_owner_token is not None
            or self._shared_workspace
            or self._sudo_action != "freeze"
            or not self.workspace_incarnation_fenced
            or self._workspace_owner_kind != "session"
            or not self._workspace_owner_id
            or not self._expected_host_key_fingerprint
            or self._workspace_tier != "sandbox"
        ):
            raise WorkspaceUnavailableError(
                "Protected workspace zero cleanup lacks exact sandbox authority"
            )
        self.shell_cleanup_strict()
        protocol = self.terminal_local_quiescence_protocol
        if protocol != "workspace_process_zero_v1":
            raise WorkspaceUnavailableError(
                "Protected workspace zero cleanup was not acknowledged"
            )
        return protocol

    @property
    def terminal_local_quiescence_protocol(self) -> str | None:
        """Return the exact proof minted by strict protected cleanup."""

        return getattr(self, "_terminal_local_quiescence_protocol", None)

    def shell_reset_after_timeout(self) -> None:
        """Stop a timed-out command before the SSH transport is recycled.

        ``disconnect()`` intentionally preserves tmux for claim handoff, so it
        cannot be timeout cancellation. A stateless owner rotates the shell's
        fenced generation under the same lease token. A pinned owner may kill
        only a session already attested to its full job/thread id. An absent
        session is already safe; a present but unattested session is never
        adopted.

        Errors deliberately propagate. Reconnecting after an unproven reset
        would let a cancelled ``sleep; touch ...`` mutate the workspace later.
        """
        if self._shell_owner_token is not None:
            self._reset_stateless_tmux_session()
            return

        expected_owner = self._job_id or self._session_name
        target = self._tmux_target()
        inner = (
            self._pinned_tmux_fence_shell()
            + f"tmux has-session -t {target} 2>/dev/null || exit 0\n"
            + "_srw_id=$(tmux display-message -p "
            + f"-t {target} '#{{{_TMUX_OWNER_ID_OPTION}}}')\n"
            + f'[ "$_srw_id" = {shlex.quote(expected_owner)} ] || exit 73\n'
            + f"tmux kill-session -t {target} || exit 77"
        )
        self._tmux_exec_checked(
            self._tmux_lock_command(inner),
            operation="reset timed-out pinned shell session",
        )
        self._shell_initialized = False
        self._tabs.clear()
        self._prompt_token = None
        self._prompt_marker = None
        self._shell_generation = None
        self._shell_protocol_current = False

    def shell_is_alive(self) -> bool:
        if not self._shell_initialized:
            return False
        try:
            output = self._exec(
                f"tmux has-session -t {self._tmux_target()} "
                "2>/dev/null && echo yes || echo no"
            )
            return output.strip() == "yes"
        except Exception:
            return False
