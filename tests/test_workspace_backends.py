"""Tests for the RemoteBackend workspace implementation.

RemoteBackend operates over SSH/SFTP via paramiko. The tests mock paramiko
entirely to avoid requiring SSH infrastructure.
"""

import errno
import io
import logging
import os
import re
import shutil
import socket
import stat as stat_module
import subprocess
import threading
import time
from contextlib import contextmanager
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from shared.runtime.core.workspace_backend import (  # noqa: E402
    RemoteCommandTimeoutError,
    WorkspaceAuthenticationError,
    WorkspaceUnavailableError,
)
from shared.runtime.core.backends.remote import WorkspaceHostIdentityMismatch  # noqa: E402
from tests._process_zero_namespace import (  # noqa: E402
    run_scenario as run_process_zero_scenario,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_sftp_attr(*, is_dir: bool = False, size: int = 0) -> MagicMock:
    """Create a mock SFTPAttributes object with proper st_mode."""
    attr = MagicMock()
    if is_dir:
        attr.st_mode = stat_module.S_IFDIR | 0o755
    else:
        attr.st_mode = stat_module.S_IFREG | 0o644
    attr.st_size = size
    return attr


def _make_sftp_entry(
    filename: str, *, is_dir: bool = False, size: int = 0
) -> MagicMock:
    """Create a mock SFTP directory entry (listdir_attr result)."""
    entry = _make_sftp_attr(is_dir=is_dir, size=size)
    entry.filename = filename
    return entry


class _WindowedChannel:
    """Mock paramiko Channel with window-full deadlock semantics.

    Exit status only becomes ready once ALL pending output has been
    recv()'d — exactly like a remote command blocked on pipe_write until
    the reader drains the channel. recv_exit_status() raises if called
    while output is undrained, which is the deadlock the fix removes
    (on real paramiko it blocks forever instead of raising).
    """

    def __init__(
        self, stdout_data=b"", stderr_data=b"", exit_code=0, never_exits=False
    ):
        self._out = stdout_data
        self._err = stderr_data
        self._exit_code = exit_code
        self._never_exits = never_exits
        self.closed = False

    def recv_ready(self):
        return bool(self._out)

    def recv(self, n):
        chunk, self._out = self._out[:n], self._out[n:]
        return chunk

    def recv_stderr_ready(self):
        return bool(self._err)

    def recv_stderr(self, n):
        chunk, self._err = self._err[:n], self._err[n:]
        return chunk

    def exit_status_ready(self):
        if self._never_exits:
            return False
        return not self._out and not self._err

    def recv_exit_status(self):
        if self._out or self._err:
            raise AssertionError(
                "recv_exit_status() called with output undrained — "
                "window-full deadlock (hangs forever on real paramiko)"
            )
        return self._exit_code

    def close(self):
        self.closed = True


def _wire_exec_channel(mock_ssh, channel):
    """Point mock_ssh.exec_command at a (stdin, stdout, stderr) triple
    whose stdout.channel is the given mock channel.

    Shell-lifecycle setup has several protocol replies that ordinary operation
    tests should not each have to recreate: the atomic creator reports
    ``created``, one pane is discovered, and setup markers are observed by the
    next capture. All other commands retain the caller-supplied channel.
    """

    def result_for(selected):
        stdout = MagicMock()
        stdout.channel = selected
        return MagicMock(), stdout, MagicMock()

    ready_marker = None

    def exec_command(command, *args, **kwargs):
        nonlocal ready_marker
        marker_match = re.search(r"(__READY_[0-9a-f]+__)", command)
        if "tmux send-keys" in command and marker_match:
            ready_marker = marker_match.group(1)

        if "tmux" in command:
            payload = b""
            stripped = command.lstrip()
            if "tmux new-session" in command and (
                "printf existing" in command or "printf created" in command
            ):
                payload = b"created"
            elif "tmux list-panes" in command:
                payload = b"%1"
            elif (
                stripped.startswith("tmux display-message")
                and _TMUX_OWNER_ID_OPTION in command
            ):
                payload = b""
            elif (
                stripped.startswith("tmux display-message")
                and _TMUX_PROMPT_TOKEN_OPTION in command
            ):
                payload = b""
            elif (
                stripped.startswith("tmux display-message")
                and _TMUX_SETUP_OPTION in command
            ):
                payload = _TMUX_SETUP_PENDING.encode()
            elif (
                stripped.startswith("tmux display-message")
                and _TMUX_PROTOCOL_OPTION in command
            ):
                payload = b""
            elif "tmux capture-pane" in command and ready_marker is not None:
                payload = f"{ready_marker} 0 /home/agent-host/workspace\n".encode()
                ready_marker = None
            elif "tmux capture-pane" in command or "tmux list-windows" in command:
                payload = channel._out
                channel._out = b""
            elif "tmux has-session" in command and "&& echo yes" in command:
                return result_for(channel)
            else:
                return result_for(_WindowedChannel())

            return result_for(_WindowedChannel(stdout_data=payload))

        return result_for(channel)

    mock_ssh.exec_command.side_effect = exec_command


class TestExecDrainLoop:
    """_exec must drain the channel and bound every wait.

    Regression tests for knowledge-base/knowledge/issues/remote_backend_indefinite_wait_deadlock.md
    (job 2dbe6854: grep output 2,319,835 B > 2 MiB window wedged a job 8 h).
    """

    def test_large_output_does_not_deadlock(self, remote_backend):
        """Output bigger than the 2 MiB channel window must be returned,
        not deadlock. Fails on pre-fix code (recv_exit_status first)."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        big = b"x" * (3 * 1024 * 1024)  # > 2 MiB window
        _wire_exec_channel(mock_ssh, _WindowedChannel(stdout_data=big))
        out = backend._exec("grep -rni role /ws")
        assert len(out) == 3 * 1024 * 1024

    def test_stderr_is_drained(self, remote_backend):
        """stderr shares the channel window; undrained stderr must not
        stall the loop even on a non-zero exit."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        chan = _WindowedChannel(
            stdout_data=b"ok", stderr_data=b"e" * 100_000, exit_code=1
        )
        _wire_exec_channel(mock_ssh, chan)
        assert backend._exec("cmd") == "ok"

    def test_timeout_raises_and_closes_channel(self, remote_backend):
        """A command that never exits must raise RemoteCommandTimeoutError
        (NOT WorkspaceUnavailableError) and close the channel."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        chan = _WindowedChannel(never_exits=True)
        _wire_exec_channel(mock_ssh, chan)
        with patch("time.sleep"):
            with pytest.raises(RemoteCommandTimeoutError):
                backend._exec("sleep 999", timeout=0)
        assert chan.closed
        assert not issubclass(RemoteCommandTimeoutError, WorkspaceUnavailableError)

    def test_output_truncated_at_cap(self, remote_backend):
        """Output beyond 5 MiB is dropped (marker appended), but the
        channel is still drained so the command can finish."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        big = b"y" * (6 * 1024 * 1024)
        _wire_exec_channel(mock_ssh, _WindowedChannel(stdout_data=big))
        out = backend._exec("cat huge")
        assert out.endswith("[output truncated at 5 MiB]")
        assert len(out) < 6 * 1024 * 1024

    def test_tail_retention_keeps_completion_data_at_end_of_large_output(
        self, remote_backend
    ):
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        big = b"x" * (6 * 1024 * 1024) + b"__SRW_COMPLETION_TAIL__"
        _wire_exec_channel(mock_ssh, _WindowedChannel(stdout_data=big))

        out, exit_code = backend._exec_with_status(
            "tmux capture-pane", retain_tail=True
        )

        assert exit_code == 0
        assert out.startswith("[output truncated at 5 MiB]")
        assert out.endswith("__SRW_COMPLETION_TAIL__")


class _InfiniteChannel(_WindowedChannel):
    """recv_ready() is always True and never exits — an infinite/fast-enough
    producer. The deadline check in the old code only ran once BOTH inner
    drain loops went empty, so a channel like this evaded the timeout
    forever. Used to prove the deadline now binds the inner loops too."""

    def __init__(self):
        super().__init__(never_exits=True)

    def recv_ready(self):
        return True

    def recv(self, n):
        return b"x" * n


class TestExecDrainLoopDeadline:
    """The wall-clock deadline must bound the inner recv/recv_stderr drain
    loops, not just the outer 'both buffers empty' check."""

    def test_infinite_producer_still_hits_deadline(self, remote_backend):
        """A channel whose recv_ready() never goes false must still raise
        RemoteCommandTimeoutError instead of looping forever."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        chan = _InfiniteChannel()
        _wire_exec_channel(mock_ssh, chan)
        with patch("time.sleep"):
            with pytest.raises(RemoteCommandTimeoutError):
                backend._exec("yes", timeout=0)
        assert chan.closed


# =============================================================================
# RemoteBackend Tests
# =============================================================================

# paramiko is installed in this environment, so RemoteBackend imports normally.
# We mock at the paramiko SSHClient/SFTPClient level rather than patching
# the module import.

from shared.runtime.core.backends.remote import (  # noqa: E402
    RemoteBackend,
    _INHERITED_BUSY_SENTINEL,
    _PENDING_GUARD_STALE_SECONDS,
    _RemoteTab,
    _TMUX_FIELD_SEPARATOR,
    _TMUX_GENERATION_OPTION,
    _TMUX_OWNER_ID_OPTION,
    _TMUX_OWNER_TOKEN_OPTION,
    _TMUX_PENDING_SENTINEL_OPTION,
    _TMUX_PENDING_SINCE_OPTION,
    _TMUX_PROTOCOL_OPTION,
    _TMUX_PROMPT_TOKEN_OPTION,
    _TMUX_SETUP_COMPLETE,
    _TMUX_SETUP_OPTION,
    _TMUX_SETUP_PENDING,
    _TMUX_TAB_TYPE_OPTION,
    _TMUX_RUNTIME_INCARNATION_OPTION,
    _TMUX_WORKSPACE_GENERATION_OPTION,
    _TMUX_WINDOW_SETUP_OPTION,
    _TCP_KEEPALIVE_COUNT,
    _TCP_KEEPALIVE_IDLE_SECONDS,
    _TCP_KEEPALIVE_INTERVAL_SECONDS,
    _TCP_USER_TIMEOUT_MILLIS,
    _TRANSPORT_KEEPALIVE_SECONDS,
    _parse_shell_completion_record,
    _sha256_public_key_fingerprint,
    _validate_private_key,
)


_WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_RUNTIME_INCARNATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


class _HostKey:
    def __init__(self, material: bytes):
        self._material = material

    def asbytes(self) -> bytes:
        return self._material


def _tmux_window_row(
    name: str,
    tab_type: str,
    pending: str = "",
    *,
    pane_count: str = "1",
    pane_id: str = "%1",
    stored_pane_id: str = "%1",
    prompt_token: str = "",
    setup_state: str = _TMUX_SETUP_COMPLETE,
) -> str:
    return _TMUX_FIELD_SEPARATOR.join(
        (
            name,
            tab_type,
            pending,
            pane_count,
            pane_id,
            stored_pane_id,
            prompt_token,
            setup_state,
        )
    )


class TestRemoteShellCompletionRecords:
    _SENTINEL = "__DONE_0123456789ab__"

    @pytest.mark.parametrize(
        "line",
        (
            "printf '__DONE_0123456789ab__ %s %s'",
            "__DONE_0123456789ab__ %s %s",
            "prefix __DONE_0123456789ab__ 0 /workspace",
            "__DONE_0123456789ab__ 0 relative/path",
            "__DONE_0123456789ab__ 999 /workspace",
        ),
    )
    def test_wrapped_or_malformed_command_echo_never_completes(self, line):
        assert _parse_shell_completion_record(line, self._SENTINEL) is None

    def test_only_exact_newline_separated_record_completes(self, remote_backend):
        backend, _, _ = remote_backend
        command, _ = backend._build_guarded_shell_command(
            "echo ok", self._SENTINEL, None
        )

        assert f"printf '\\n{self._SENTINEL}" in command
        assert _parse_shell_completion_record(
            f"{self._SENTINEL} 0 /workspace", self._SENTINEL
        ) == (0, "/workspace")


@pytest.fixture(autouse=True)
def _valid_private_key_for_mocked_ssh():
    """Most tests exercise mocked transport behavior, not key parsing."""
    with patch(
        "shared.runtime.core.backends.remote._validate_private_key",
        return_value="SHA256:test-fingerprint",
    ):
        yield


@pytest.fixture
def remote_backend():
    """Create a RemoteBackend instance with mocked SSH/SFTP.

    Patches paramiko.SSHClient so that connect() uses our mock objects.
    """
    mock_ssh = MagicMock()
    mock_sftp = MagicMock()
    mock_transport = MagicMock()
    mock_transport.is_active.return_value = True
    mock_ssh.get_transport.return_value = mock_transport
    mock_ssh.open_sftp.return_value = mock_sftp

    with patch("paramiko.SSHClient", return_value=mock_ssh):
        backend = RemoteBackend(
            host="10.0.0.42",
            port=22,
            username="agent-host",
            key_path="/run/secrets/test-workspace-key",
            workspace_path="/home/agent-host/workspace",
            job_id="aaaa-bbbb-cccc-dddd",
            max_retries=2,
            connect_timeout=5,
        )
        yield backend, mock_ssh, mock_sftp


class TestRemoteBackendInit:
    """Tests for RemoteBackend initialization."""

    def test_init_stores_parameters(self):
        backend = RemoteBackend(
            host="192.168.1.100",
            port=2222,
            username="testuser",
            workspace_path="/opt/workspace/",
            job_id="test-job-123",
            scrollback_limit=3000,
            default_timeout=60,
            max_tabs=10,
        )
        assert backend._host == "192.168.1.100"
        assert backend._port == 2222
        assert backend._username == "testuser"
        assert backend._remote_root == "/opt/workspace"  # trailing slash stripped
        assert backend._job_id == "test-job-123"
        assert backend._scrollback_limit == 3000
        assert backend._default_timeout == 60
        assert backend._max_tabs == 10

    def test_init_default_blocked_commands(self):
        backend = RemoteBackend(host="host", workspace_path="/ws")
        assert "reboot" in backend._blocked_commands
        assert "shutdown" in backend._blocked_commands
        assert "poweroff" in backend._blocked_commands

    def test_init_custom_blocked_commands(self):
        backend = RemoteBackend(
            host="host", workspace_path="/ws", blocked_commands=["rm", "dd"]
        )
        assert backend._blocked_commands == frozenset(["rm", "dd"])

    def test_init_without_paramiko_raises(self):
        """RemoteBackend.__init__ raises when paramiko is None at module level."""
        import shared.runtime.core.backends.remote as remote_mod

        original = remote_mod.paramiko
        try:
            remote_mod.paramiko = None
            with pytest.raises(ImportError, match="paramiko is required"):
                RemoteBackend(host="host", workspace_path="/ws")
        finally:
            remote_mod.paramiko = original

    def test_root_property(self):
        backend = RemoteBackend(
            host="host", workspace_path="/home/agent-host/workspace"
        )
        assert backend.root == "/home/agent-host/workspace"

    def test_supports_shell(self):
        backend = RemoteBackend(host="host", workspace_path="/ws")
        assert backend.supports_shell is True

    def test_session_name_from_job_id(self):
        backend = RemoteBackend(
            host="host", workspace_path="/ws", job_id="abcdef123456-rest"
        )
        assert backend._session_name == "agent_abcdef123456"

    def test_session_name_without_job_id(self):
        backend = RemoteBackend(host="host", workspace_path="/ws", job_id="")
        assert backend._session_name == "agent_remote"

    @pytest.mark.parametrize(
        ("workspace_generation", "runtime_incarnation"),
        [(_WORKSPACE_GENERATION, None), (None, _RUNTIME_INCARNATION)],
    )
    def test_workspace_authority_must_be_supplied_as_a_pair(
        self, workspace_generation, runtime_incarnation
    ):
        with pytest.raises(ValueError, match="must be supplied together"):
            RemoteBackend(
                host="host",
                workspace_path="/ws",
                workspace_generation=workspace_generation,
                runtime_incarnation=runtime_incarnation,
            )

    def test_workspace_owner_authority_requires_incarnation_fence(self):
        with pytest.raises(ValueError, match="requires generation and runtime"):
            RemoteBackend(
                host="host",
                workspace_path="/ws",
                job_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
                workspace_owner_kind="job",
                workspace_owner_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            )

    @pytest.mark.parametrize(
        "fingerprint",
        [
            "MD5:legacy",
            "SHA256:has space",
            "SHA256:short",
            "SHA256:" + "A" * 43 + "=",
        ],
    )
    def test_expected_host_key_requires_sha256_fingerprint(self, fingerprint):
        with pytest.raises(
            WorkspaceAuthenticationError, match="OpenSSH SHA256 fingerprint"
        ):
            RemoteBackend(
                host="host",
                workspace_path="/ws",
                expected_host_key_fingerprint=fingerprint,
            )

    def test_required_kubernetes_host_identity_has_no_autoadd_fallback(self):
        with pytest.raises(
            WorkspaceAuthenticationError,
            match="requires an orchestrator-attested SSH host key fingerprint",
        ):
            RemoteBackend(
                host="workspace.test",
                workspace_path="/ws",
                require_host_key_fingerprint=True,
            )


class TestRemoteBackendConnect:
    """Tests for RemoteBackend.connect()."""

    def test_connect_success(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        assert backend._ssh is not None
        assert backend._sftp is not None
        mock_ssh.set_missing_host_key_policy.assert_called_once()
        mock_ssh.connect.assert_called_once_with(
            hostname="10.0.0.42",
            port=22,
            username="agent-host",
            timeout=5,
            key_filename="/run/secrets/test-workspace-key",
            allow_agent=False,
            look_for_keys=False,
        )

    def test_connect_accepts_exact_pinned_host_key(self):
        host_key = _HostKey(b"pod-u1-host-key")
        expected = _sha256_public_key_fingerprint(host_key)
        mock_ssh = MagicMock()
        mock_sftp = MagicMock()
        mock_ssh.open_sftp.return_value = mock_sftp
        mock_ssh.get_transport.return_value.is_active.return_value = True

        def present_host_key(**_kwargs):
            policy = mock_ssh.set_missing_host_key_policy.call_args.args[0]
            policy.missing_host_key(mock_ssh, "workspace.test", host_key)

        mock_ssh.connect.side_effect = present_host_key
        with patch("paramiko.SSHClient", return_value=mock_ssh):
            backend = RemoteBackend(
                host="workspace.test",
                key_path="/key",
                workspace_path="/ws",
                max_retries=5,
                expected_host_key_fingerprint=expected,
            )
            backend.connect()

        mock_ssh.connect.assert_called_once()
        mock_ssh.open_sftp.assert_called_once()

    def test_connect_rejects_host_key_mismatch_before_sftp(self):
        expected = _sha256_public_key_fingerprint(_HostKey(b"pod-u1-host-key"))
        replacement_key = _HostKey(b"pod-u2-host-key")
        mock_ssh = MagicMock()

        def present_replacement_key(**_kwargs):
            policy = mock_ssh.set_missing_host_key_policy.call_args.args[0]
            policy.missing_host_key(mock_ssh, "workspace.test", replacement_key)

        mock_ssh.connect.side_effect = present_replacement_key
        with patch("paramiko.SSHClient", return_value=mock_ssh):
            backend = RemoteBackend(
                host="workspace.test",
                key_path="/key",
                workspace_path="/ws",
                max_retries=5,
                expected_host_key_fingerprint=expected,
            )
            with pytest.raises(
                WorkspaceHostIdentityMismatch,
                match="host key fingerprint mismatch",
            ) as mismatch:
                backend.connect()

        # Identity mismatch is terminal: no retries and no workspace channel.
        assert isinstance(mismatch.value, WorkspaceAuthenticationError)
        mock_ssh.connect.assert_called_once()
        mock_ssh.open_sftp.assert_not_called()
        mock_ssh.close.assert_called()

    def test_connect_with_key_path(self):
        mock_ssh = MagicMock()
        mock_sftp = MagicMock()
        mock_ssh.open_sftp.return_value = mock_sftp

        with patch("paramiko.SSHClient", return_value=mock_ssh):
            backend = RemoteBackend(
                host="10.0.0.42",
                workspace_path="/ws",
                key_path="/home/user/.ssh/id_rsa",
                max_retries=1,
            )
            backend.connect()
        connect_call = mock_ssh.connect.call_args
        assert connect_call.kwargs.get("key_filename") == "/home/user/.ssh/id_rsa"

    def test_connect_retries_on_failure(self, remote_backend):
        """connect() retries with exponential backoff on SSH errors."""
        import paramiko as real_paramiko

        backend, mock_ssh, mock_sftp = remote_backend

        # First call fails, second succeeds
        mock_ssh.connect.side_effect = [
            real_paramiko.SSHException("connection refused"),
            None,
        ]

        with patch("time.sleep"):
            backend.connect()

        assert mock_ssh.connect.call_count == 2

    def test_connect_raises_after_max_retries(self, remote_backend):
        """connect() raises WorkspaceUnavailableError after all retries exhausted."""
        backend, mock_ssh, mock_sftp = remote_backend

        mock_ssh.connect.side_effect = socket.error("no route to host")

        with patch("time.sleep"):
            with pytest.raises(WorkspaceUnavailableError, match="Failed to connect"):
                backend.connect()

        # max_retries = 2, so 2 attempts
        assert mock_ssh.connect.call_count == 2


class TestRemoteBackendConnectClassification:
    """connect() classifies the failure cause and sizes the retry budget."""

    @contextmanager
    def _scenario(self, side_effect, retries):
        """Yield (backend, mock_ssh) with paramiko.SSHClient AND time.sleep
        patched for the whole scope — connect() must run while paramiko is
        mocked, or it dials a real host (5s network timeout)."""
        mock_ssh = MagicMock()
        mock_ssh.open_sftp.return_value = MagicMock()
        mock_ssh.connect.side_effect = side_effect
        with patch("paramiko.SSHClient", return_value=mock_ssh), patch("time.sleep"):
            backend = RemoteBackend(
                host="10.0.0.42",
                workspace_path="/ws",
                connect_timeout=5,
                max_retries=retries,
            )
            yield backend, mock_ssh

    def test_gone_dns_fails_fast_no_retry(self):
        """gaierror (NXDOMAIN) = pod gone → raise on the first attempt."""
        err = socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        with self._scenario(err, 5) as (backend, mock_ssh):
            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()
            assert mock_ssh.connect.call_count == 1

    def test_authentication_failure_is_terminal_without_retry(self):
        import paramiko as real_paramiko

        err = real_paramiko.AuthenticationException("Authentication failed")
        with self._scenario(err, 5) as (backend, mock_ssh):
            with pytest.raises(
                WorkspaceAuthenticationError, match="authentication failed"
            ):
                backend.connect()
            assert mock_ssh.connect.call_count == 1

    def test_no_authentication_methods_symptom_is_terminal_without_retry(self):
        import paramiko as real_paramiko

        err = real_paramiko.SSHException("No authentication methods available")
        with self._scenario(err, 5) as (backend, mock_ssh):
            with pytest.raises(WorkspaceAuthenticationError):
                backend.connect()
            assert mock_ssh.connect.call_count == 1

    def test_no_route_fails_fast(self):
        """OSError EHOSTUNREACH = no route → gone → fail fast."""
        err = OSError(errno.EHOSTUNREACH, "No route to host")
        with self._scenario(err, 5) as (backend, mock_ssh):
            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()
            assert mock_ssh.connect.call_count == 1

    def test_connection_refused_uses_full_boot_budget(self):
        """ECONNREFUSED = sshd booting → keep the full max_retries budget."""
        err = ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
        with self._scenario(err, 5) as (backend, mock_ssh):
            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()
            assert mock_ssh.connect.call_count == 5

    def test_timeout_capped_at_two(self):
        """Ambiguous (timeout) → short cap, not the full budget."""
        with self._scenario(socket.timeout("timed out"), 5) as (backend, mock_ssh):
            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()
            assert mock_ssh.connect.call_count == 2

    def test_vm_timeout_can_use_full_boot_budget(self):
        """VM-over-tailnet timeout can represent boot/peer convergence."""
        err = socket.timeout("timed out")
        with self._scenario(err, 5) as (backend, mock_ssh):
            backend._retry_timeouts_as_booting = True
            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()
            assert mock_ssh.connect.call_count == 5

    def test_vm_timeout_budget_is_first_connect_only(self):
        """After one successful SSH session, VM timeouts use the short cap."""
        mock_ssh = MagicMock()
        mock_ssh.open_sftp.return_value = MagicMock()
        with patch("paramiko.SSHClient", return_value=mock_ssh), patch("time.sleep"):
            backend = RemoteBackend(
                host="10.0.0.42",
                workspace_path="/ws",
                connect_timeout=5,
                max_retries=5,
                retry_timeouts_as_booting=True,
            )
            backend.connect()
            mock_ssh.connect.reset_mock()
            mock_ssh.connect.side_effect = socket.timeout("timed out")

            with pytest.raises(WorkspaceUnavailableError):
                backend.connect()

            assert mock_ssh.connect.call_count == 2

    def test_message_says_workspace_not_vm(self):
        """Renamed error string: 'workspace', never 'VM'."""
        err = socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        with self._scenario(err, 1) as (backend, mock_ssh):
            with pytest.raises(WorkspaceUnavailableError) as exc:
                backend.connect()
        assert "workspace" in str(exc.value)
        assert "VM" not in str(exc.value)


class TestRemotePrivateKeyValidation:
    """Credential configuration fails before Paramiko tries ambient identities."""

    def test_missing_key_path_is_explicit_authentication_error(self):
        with pytest.raises(WorkspaceAuthenticationError, match="key_path is missing"):
            _validate_private_key(None)

    def test_nonexistent_key_path_is_explicit_authentication_error(self, tmp_path):
        missing = tmp_path / "missing-key"
        with pytest.raises(WorkspaceAuthenticationError, match="does not exist"):
            _validate_private_key(str(missing))

    def test_invalid_key_is_not_reported_as_workspace_unavailable(self, tmp_path):
        invalid = tmp_path / "invalid-key"
        invalid.write_text("definitely not a private key", encoding="utf-8")
        with pytest.raises(WorkspaceAuthenticationError, match="invalid"):
            _validate_private_key(str(invalid))


class TestRemoteBackendDisconnect:
    """Tests for RemoteBackend.disconnect()."""

    def test_disconnect_closes_resources(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        backend.disconnect()
        mock_sftp.close.assert_called_once()
        mock_ssh.close.assert_called_once()
        assert backend._ssh is None
        assert backend._sftp is None

    def test_disconnect_preserves_tmux_session(self, remote_backend):
        """Transport detach drops local state without destroying remote tmux."""
        backend, _, _ = remote_backend
        backend.connect()
        backend._shell_initialized = True
        backend._tabs["default"] = MagicMock()

        with patch.object(backend, "_exec") as execute:
            backend.disconnect()

        execute.assert_not_called()
        assert backend._shell_initialized is False
        assert len(backend._tabs) == 0

    def test_disconnect_without_connect_is_safe(self, remote_backend):
        """disconnect() on a never-connected backend should not raise."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.disconnect()  # Should not raise

    def test_disconnect_handles_sftp_close_error(self, remote_backend):
        """disconnect() handles errors during SFTP close gracefully."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.close.side_effect = Exception("already closed")
        backend.disconnect()  # Should not raise
        assert backend._sftp is None

    def test_retired_backend_cannot_reconnect(self, remote_backend):
        backend, _, _ = remote_backend
        backend.connect()
        backend.retire()

        with patch.object(backend, "_connect_impl") as connect_impl:
            with pytest.raises(WorkspaceUnavailableError, match="retired"):
                backend._ensure_connected()
            with pytest.raises(WorkspaceUnavailableError, match="retired"):
                backend.connect()

        connect_impl.assert_not_called()
        assert backend.is_connected() is False

    def test_retire_serializes_behind_inflight_connect(self, remote_backend):
        backend, _, _ = remote_backend
        connect_entered = threading.Event()
        release_connect = threading.Event()
        retire_finished = threading.Event()
        errors = []

        def slow_connect():
            connect_entered.set()
            if not release_connect.wait(timeout=2):
                raise AssertionError("test did not release connect barrier")

        def connect_worker():
            try:
                backend.connect()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        def retire_worker():
            try:
                backend.retire()
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)
            finally:
                retire_finished.set()

        with patch.object(backend, "_connect_impl", side_effect=slow_connect):
            connecting = threading.Thread(target=connect_worker)
            retiring = threading.Thread(target=retire_worker)
            connecting.start()
            assert connect_entered.wait(timeout=1)
            retiring.start()
            assert not retire_finished.wait(timeout=0.05)
            release_connect.set()
            connecting.join(timeout=1)
            retiring.join(timeout=1)

        assert errors == []
        assert retire_finished.is_set()
        assert backend._retired is True
        with pytest.raises(WorkspaceUnavailableError, match="retired"):
            backend._ensure_connected()


class TestRemoteBackendIsConnected:
    """Tests for RemoteBackend.is_connected()."""

    def test_connected_after_connect(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        assert backend.is_connected() is True

    def test_not_connected_initially(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        assert backend.is_connected() is False

    def test_not_connected_when_transport_inactive(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        # Simulate transport becoming inactive
        transport = mock_ssh.get_transport.return_value
        transport.is_active.return_value = False
        assert backend.is_connected() is False

    def test_not_connected_when_transport_none(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_ssh.get_transport.return_value = None
        assert backend.is_connected() is False


class TestRemoteBackendEnsureConnected:
    """Tests for RemoteBackend._ensure_connected()."""

    def test_noop_when_connected(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        # Reset call count
        mock_ssh.connect.reset_mock()
        backend._ensure_connected()
        # Should not attempt reconnection
        mock_ssh.connect.assert_not_called()

    def test_reconnects_when_disconnected(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Simulate connection drop
        transport = mock_ssh.get_transport.return_value
        transport.is_active.return_value = False

        # After reconnect, transport is active again
        def restore_active(*args, **kwargs):
            transport.is_active.return_value = True

        mock_ssh.connect.side_effect = restore_active

        with patch("time.sleep"):
            backend._ensure_connected()

    def test_reconnect_identity_rotation_requests_fresh_attestation_before_io(self):
        original_key = _HostKey(b"pod-u1-host-key")
        replacement_key = _HostKey(b"pod-u2-host-key")
        expected = _sha256_public_key_fingerprint(original_key)

        first_ssh = MagicMock()
        first_sftp = MagicMock()
        first_transport = MagicMock()
        first_transport.is_active.return_value = True
        first_ssh.open_sftp.return_value = first_sftp
        first_ssh.get_transport.return_value = first_transport

        second_ssh = MagicMock()
        second_ssh.get_transport.return_value.is_active.return_value = True

        def present(client, key):
            def _connect(**_kwargs):
                policy = client.set_missing_host_key_policy.call_args.args[0]
                policy.missing_host_key(client, "workspace.test", key)

            return _connect

        first_ssh.connect.side_effect = present(first_ssh, original_key)
        second_ssh.connect.side_effect = present(second_ssh, replacement_key)

        with patch("paramiko.SSHClient", side_effect=[first_ssh, second_ssh]):
            backend = RemoteBackend(
                host="workspace.test",
                key_path="/key",
                workspace_path="/ws",
                max_retries=5,
                expected_host_key_fingerprint=expected,
            )
            backend.connect()
            reconnect_hook = MagicMock()
            backend.set_reconnect_hook(reconnect_hook)
            first_transport.is_active.return_value = False

            with pytest.raises(
                WorkspaceUnavailableError,
                match="fresh workspace attestation is required",
            ) as unavailable:
                # Exercise the public workspace-I/O path: no SFTP byte may
                # cross to U2 under U1's host-key/runtime attestation.
                backend.read_file("handoff.txt")

        assert isinstance(unavailable.value.__cause__, WorkspaceHostIdentityMismatch)
        second_ssh.connect.assert_called_once()
        second_ssh.open_sftp.assert_not_called()
        reconnect_hook.assert_not_called()

    def test_reconnect_genuine_auth_failure_keeps_authentication_classification(
        self, remote_backend
    ):
        import paramiko as real_paramiko

        backend, mock_ssh, _mock_sftp = remote_backend
        backend.connect()
        transport = mock_ssh.get_transport.return_value
        transport.is_active.return_value = False
        mock_ssh.connect.side_effect = real_paramiko.AuthenticationException(
            "Authentication failed"
        )

        with pytest.raises(
            WorkspaceAuthenticationError, match="authentication failed"
        ) as auth:
            backend.read_file("must-not-open.txt")

        assert not isinstance(auth.value, WorkspaceUnavailableError)

    def test_raises_after_reconnect_failure(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Simulate connection drop
        transport = mock_ssh.get_transport.return_value
        transport.is_active.return_value = False

        # All reconnect attempts fail
        mock_ssh.connect.side_effect = socket.error("unreachable")

        with patch("time.sleep"):
            with pytest.raises(WorkspaceUnavailableError):
                backend._ensure_connected()

    def test_ensure_connected_does_not_multiply_retry_budget(self, remote_backend):
        """_ensure_connected must not wrap connect()'s own retry loop in a second
        loop — a dead host should cost max_retries attempts, not max_retries²."""
        backend, mock_ssh, mock_sftp = remote_backend  # fixture: max_retries=2
        backend._ssh = None  # force is_connected() False → reconnect path
        mock_ssh.connect.side_effect = socket.error("host down")

        with patch("time.sleep"):
            with pytest.raises(WorkspaceUnavailableError):
                backend._ensure_connected()

        # max_retries=2 → 2 attempts. The nested bug produced 2*2 = 4.
        assert mock_ssh.connect.call_count == 2


class TestRemoteBackendPathResolution:
    """Tests for RemoteBackend._resolve() and resolve_path()."""

    def test_resolve_empty_returns_root(self, remote_backend):
        backend, _, _ = remote_backend
        assert backend._resolve("") == "/home/agent-host/workspace"

    def test_resolve_dot_returns_root(self, remote_backend):
        backend, _, _ = remote_backend
        assert backend._resolve(".") == "/home/agent-host/workspace"

    def test_resolve_simple_path(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._resolve("docs/file.txt")
        assert result == "/home/agent-host/workspace/docs/file.txt"

    def test_resolve_rejects_parent_traversal(self, remote_backend):
        backend, _, _ = remote_backend
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend._resolve("../../etc/passwd")

    def test_resolve_rejects_absolute_path(self, remote_backend):
        backend, _, _ = remote_backend
        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend._resolve("/etc/passwd")

    def test_resolve_normalizes_dots(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._resolve("a/./b/../c")
        assert result == "/home/agent-host/workspace/a/c"

    def test_resolve_path_method(self, remote_backend):
        backend, _, _ = remote_backend
        assert backend.resolve_path("test.txt") == "/home/agent-host/workspace/test.txt"


class TestRemoteBackendReadFile:
    """Tests for RemoteBackend.read_file()."""

    def _setup_connected(self, backend, mock_ssh, mock_sftp):
        """Connect the backend and set up transport as active."""
        backend.connect()

    def test_read_text_file(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        self._setup_connected(backend, mock_ssh, mock_sftp)

        file_data = b"Hello, remote world!"
        file_obj = io.BytesIO(file_data)
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        content = backend.read_file("hello.txt")
        assert content == "Hello, remote world!"
        mock_sftp.open.assert_called_with("/home/agent-host/workspace/hello.txt", "rb")

    def test_read_binary_file(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        self._setup_connected(backend, mock_ssh, mock_sftp)

        file_data = b"\x89PNG\r\n"
        file_obj = io.BytesIO(file_data)
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        content = backend.read_file("image.png", binary=True)
        assert content == file_data
        assert isinstance(content, bytes)

    def test_read_nonexistent_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        self._setup_connected(backend, mock_ssh, mock_sftp)

        mock_sftp.open.side_effect = FileNotFoundError("no such file")
        with pytest.raises(FileNotFoundError, match="File not found"):
            backend.read_file("ghost.txt")

    def test_read_ioerror_raises_file_not_found(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        self._setup_connected(backend, mock_ssh, mock_sftp)

        mock_sftp.open.side_effect = IOError("permission denied")
        with pytest.raises(FileNotFoundError, match="Cannot read"):
            backend.read_file("restricted.txt")

    def test_read_path_traversal_rejected(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        self._setup_connected(backend, mock_ssh, mock_sftp)

        with pytest.raises(ValueError, match="escapes workspace boundary"):
            backend.read_file("../../etc/shadow")


class TestRemoteBackendWriteFile:
    """Tests for RemoteBackend.write_file()."""

    def test_write_text_file(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Mock the parent directory check
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)

        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        backend.write_file("output.txt", "Hello!")
        mock_sftp.open.assert_called_with("/home/agent-host/workspace/output.txt", "wb")

    def test_write_binary_content(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        data = b"\x00\x01\x02"
        backend.write_file("data.bin", data)
        file_obj.write.assert_called_once_with(data)

    def test_write_text_encoded_as_utf8(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        backend.write_file("text.txt", "Hello")
        file_obj.write.assert_called_once_with(b"Hello")

    def test_write_file_timeout_raises(self, remote_backend):
        """A stalled sftp.open() during a write must surface as
        RemoteCommandTimeoutError, not an opaque/uncaught socket.timeout."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        mock_sftp.open.side_effect = socket.timeout("timed out")

        with pytest.raises(RemoteCommandTimeoutError, match="timed out writing"):
            backend.write_file("output.txt", "Hello!")


class TestRemoteBackendHomeFile:
    """Tests for RemoteBackend.write_home_file() and resolve_home_path()."""

    def _setup(self, remote_backend, *, home: str = "/home/agent-host"):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.normalize.return_value = home
        # Make _ensure_remote_dir's stat probe see existing parents so the
        # mkdir loop terminates immediately.
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        return backend, mock_ssh, mock_sftp

    def test_resolve_home_path_joins_home(self, remote_backend):
        backend, _, mock_sftp = self._setup(remote_backend)
        assert backend.resolve_home_path(".ssh/repo_x") == (
            "/home/agent-host/.ssh/repo_x"
        )
        # Cached after first call: normalize called exactly once.
        backend.resolve_home_path(".ssh/repo_y")
        mock_sftp.normalize.assert_called_once_with(".")

    def test_resolve_home_path_normalizes(self, remote_backend):
        backend, _, _ = self._setup(remote_backend)
        assert (
            backend.resolve_home_path(".ssh/./nested/../repo")
            == "/home/agent-host/.ssh/repo"
        )

    def test_resolve_home_path_rejects_empty(self, remote_backend):
        backend, _, _ = self._setup(remote_backend)
        with pytest.raises(ValueError, match="non-empty"):
            backend.resolve_home_path("")
        with pytest.raises(ValueError, match="non-empty"):
            backend.resolve_home_path(".")

    def test_resolve_home_path_rejects_absolute(self, remote_backend):
        backend, _, _ = self._setup(remote_backend)
        with pytest.raises(ValueError, match="escapes home directory"):
            backend.resolve_home_path("/etc/passwd")

    def test_resolve_home_path_rejects_traversal(self, remote_backend):
        backend, _, _ = self._setup(remote_backend)
        with pytest.raises(ValueError, match="escapes home directory"):
            backend.resolve_home_path("../../etc/passwd")

    def test_write_home_file_writes_under_home(self, remote_backend):
        backend, _, mock_sftp = self._setup(remote_backend)
        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        backend.write_home_file(".ssh/repo_foo", "PRIVATE KEY")
        mock_sftp.open.assert_called_with("/home/agent-host/.ssh/repo_foo", "wb")
        file_obj.write.assert_called_once_with(b"PRIVATE KEY")

    def test_write_home_file_accepts_bytes(self, remote_backend):
        backend, _, mock_sftp = self._setup(remote_backend)
        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        backend.write_home_file(".ssh/repo_bin", b"\x00\xff")
        file_obj.write.assert_called_once_with(b"\x00\xff")

    def test_write_home_file_rejects_escape(self, remote_backend):
        backend, _, _ = self._setup(remote_backend)
        with pytest.raises(ValueError, match="escapes home directory"):
            backend.write_home_file("/etc/passwd", "x")
        with pytest.raises(ValueError, match="escapes home directory"):
            backend.write_home_file("../escape", "x")


class TestRemoteBackendAppendFile:
    """Tests for RemoteBackend.append_file()."""

    def test_append_opens_in_append_mode(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        file_obj = MagicMock()
        mock_sftp.open.return_value.__enter__ = MagicMock(return_value=file_obj)
        mock_sftp.open.return_value.__exit__ = MagicMock(return_value=False)

        backend.append_file("log.txt", "new line\n")
        mock_sftp.open.assert_called_with("/home/agent-host/workspace/log.txt", "ab")
        file_obj.write.assert_called_once_with(b"new line\n")


class TestRemoteBackendExists:
    """Tests for RemoteBackend.exists(), is_file(), is_dir()."""

    def test_exists_true(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr()
        assert backend.exists("file.txt") is True

    def test_exists_false(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = FileNotFoundError()
        assert backend.exists("ghost.txt") is False

    def test_is_file_true(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        assert backend.is_file("file.txt") is True

    def test_is_file_false_for_dir(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        assert backend.is_file("dir") is False

    def test_is_file_false_for_nonexistent(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = FileNotFoundError()
        assert backend.is_file("ghost.txt") is False

    def test_is_dir_true(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        assert backend.is_dir("subdir") is True

    def test_is_dir_false_for_file(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        assert backend.is_dir("file.txt") is False

    def test_is_dir_false_for_nonexistent(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = FileNotFoundError()
        assert backend.is_dir("ghost") is False


class TestRemoteBackendListDir:
    """Tests for RemoteBackend.list_dir()."""

    def test_list_dir_with_entries(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # stat returns directory
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)

        entries = [
            _make_sftp_entry("file.txt", is_dir=False),
            _make_sftp_entry("subdir", is_dir=True),
        ]
        mock_sftp.listdir_attr.return_value = entries

        result = backend.list_dir("")
        assert "file.txt" in result
        assert "subdir/" in result

    def test_list_dir_nonexistent_returns_empty(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = FileNotFoundError()
        result = backend.list_dir("ghost_dir")
        assert result == []

    def test_list_dir_not_a_directory(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        result = backend.list_dir("file.txt")
        assert result == ["file.txt"]

    def test_list_dir_with_pattern(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        entries = [
            _make_sftp_entry("a.py", is_dir=False),
            _make_sftp_entry("b.py", is_dir=False),
            _make_sftp_entry("c.txt", is_dir=False),
        ]
        mock_sftp.listdir_attr.return_value = entries

        result = backend.list_dir("", pattern="*.py")
        assert "a.py" in result
        assert "b.py" in result
        assert "c.txt" not in result

    def test_list_dir_sorted(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        entries = [
            _make_sftp_entry("z.txt", is_dir=False),
            _make_sftp_entry("a.txt", is_dir=False),
            _make_sftp_entry("m.txt", is_dir=False),
        ]
        mock_sftp.listdir_attr.return_value = entries

        result = backend.list_dir("")
        assert result == sorted(result)

    def test_list_dir_ioerror_returns_empty(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        mock_sftp.listdir_attr.side_effect = IOError("permission denied")

        result = backend.list_dir("")
        assert result == []


class TestSftpTimeoutNotSwallowed:
    """socket.timeout is an IOError subclass. Now that the SFTP channel
    carries a 60s timeout (TestConnectionHardening), a stalled channel
    reaches these ``except IOError`` sites and must NOT be reported as
    "path doesn't exist" / "empty directory" / "already exists" — it must
    surface as RemoteCommandTimeoutError, same as read_file already does.
    """

    def test_remote_stat_timeout_raises_via_exists(self, remote_backend):
        """A stalled stat() must not read as 'file doesn't exist'."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = socket.timeout("timed out")
        with pytest.raises(RemoteCommandTimeoutError, match="timed out"):
            backend.exists("file.txt")

    def test_list_dir_listdir_attr_timeout_raises(self, remote_backend):
        """A stalled listdir_attr() must not read as 'empty directory'."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        mock_sftp.listdir_attr.side_effect = socket.timeout("timed out")
        with pytest.raises(RemoteCommandTimeoutError, match="timed out"):
            backend.list_dir("")

    def test_ensure_remote_dir_mkdir_timeout_raises(self, remote_backend):
        """A stalled mkdir() inside _ensure_remote_dir must not be
        swallowed as 'race condition or already exists'."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = FileNotFoundError()  # no existing ancestor
        mock_sftp.mkdir.side_effect = socket.timeout("timed out")
        with pytest.raises(RemoteCommandTimeoutError, match="timed out"):
            backend.mkdir("new_dir")


class TestRemoteBackendSearchFiles:
    """Tests for RemoteBackend.search_files() (server-side grep)."""

    def _setup_exec(self, mock_ssh, output: str, exit_code: int = 0):
        """Set up mock exec_command to return given output."""
        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(stdout_data=output.encode("utf-8"), exit_code=exit_code),
        )

    def test_search_parses_grep_output(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        grep_output = (
            "/home/agent-host/workspace/file.txt:5:found the target here\n"
            "/home/agent-host/workspace/sub/other.py:12:another target line\n"
        )
        self._setup_exec(mock_ssh, grep_output)

        results = backend.search_files("target")
        assert len(results) == 2
        assert results[0]["path"] == "file.txt"
        assert results[0]["line_number"] == 5
        assert "target" in results[0]["line"]
        assert results[1]["path"] == "sub/other.py"
        assert results[1]["line_number"] == 12

    def test_search_no_results(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")
        results = backend.search_files("nonexistent")
        assert results == []

    def test_search_case_insensitive_flag(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")

        backend.search_files("Test", case_sensitive=False)
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "-rni" in cmd

    def test_search_case_sensitive_flag(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")

        backend.search_files("Test", case_sensitive=True)
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "-rn " in cmd  # no 'i' flag
        assert "-rni" not in cmd

    def test_search_escapes_single_quotes(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")

        backend.search_files("it's a test")
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "'\\''" in cmd  # escaped single quote

    def test_search_skips_malformed_lines(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        grep_output = (
            "malformed line without colons\n"
            "/home/agent-host/workspace/good.txt:1:valid result\n"
            "\n"
        )
        self._setup_exec(mock_ssh, grep_output)

        results = backend.search_files("query")
        assert len(results) == 1
        assert results[0]["path"] == "good.txt"

    def test_search_with_exclude_dirs_includes_flags(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")
        exclude_dirs = ["node_modules", ".git"]

        backend.search_files("needle", exclude_dirs=exclude_dirs)
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "--exclude-dir='node_modules'" in cmd
        assert "--exclude-dir='.git'" in cmd

    def test_search_handles_invalid_line_number(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        grep_output = "/home/agent-host/workspace/file.txt:notanum:some content\n"
        self._setup_exec(mock_ssh, grep_output)

        results = backend.search_files("query")
        assert results == []

    def test_search_escapes_single_quotes_in_exclude_dirs(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec(mock_ssh, "")
        backend.search_files("needle", exclude_dirs=["foo'bar"])

        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "--exclude-dir='foo'\\''bar'" in cmd


class TestSearchFilesCap:
    """search_files must bound grep output server-side: the display cap
    is 50 matches, yet uncapped grep shipped 2.2 MB in the incident."""

    def test_grep_command_is_head_capped(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        with patch.object(backend, "_exec", return_value="") as ex:
            backend.search_files("role")
        cmd = ex.call_args.args[0]
        assert "| head -n 2000" in cmd
        assert cmd.rstrip().endswith("|| true")


class TestRemoteBackendMkdir:
    """Tests for RemoteBackend.mkdir()."""

    def test_mkdir_calls_ensure_remote_dir(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # First call to stat returns None (doesn't exist), second returns dir
        call_count = [0]
        root_attr = _make_sftp_attr(is_dir=True)

        def stat_side_effect(path):
            call_count[0] += 1
            if path == "/home/agent-host/workspace/new_dir":
                raise FileNotFoundError()
            return root_attr

        mock_sftp.stat.side_effect = stat_side_effect
        backend.mkdir("new_dir")
        mock_sftp.mkdir.assert_called()


class TestRemoteBackendDeleteFile:
    """Tests for RemoteBackend.delete_file()."""

    def test_delete_existing_file(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        result = backend.delete_file("target.txt")
        assert result is True
        mock_sftp.remove.assert_called_once_with(
            "/home/agent-host/workspace/target.txt"
        )

    def test_delete_nonexistent_returns_false(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.side_effect = FileNotFoundError()
        result = backend.delete_file("ghost.txt")
        assert result is False

    def test_delete_empty_directory(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        mock_sftp.listdir.return_value = []
        result = backend.delete_file("empty_dir")
        assert result is True
        mock_sftp.rmdir.assert_called_once()

    def test_delete_nonempty_directory_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        mock_sftp.listdir.return_value = ["child.txt"]
        with pytest.raises(ValueError, match="Cannot delete non-empty directory"):
            backend.delete_file("nonempty_dir")


class TestRemoteBackendDeleteDirectory:
    """Tests for RemoteBackend.delete_directory()."""

    def test_delete_directory_uses_rm_rf(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)

        # Mock exec for rm -rf
        _wire_exec_channel(mock_ssh, _WindowedChannel())

        result = backend.delete_directory("tree")
        assert result is True
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "rm -rf" in cmd

    def test_delete_root_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        with pytest.raises(ValueError, match="Cannot delete workspace root"):
            backend.delete_directory("")

    def test_delete_nonexistent_returns_false(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.side_effect = FileNotFoundError()
        result = backend.delete_directory("ghost_dir")
        assert result is False

    def test_delete_file_as_directory_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        with pytest.raises(ValueError, match="Not a directory"):
            backend.delete_directory("file.txt")


class TestRemoteBackendMove:
    """Tests for RemoteBackend.move()."""

    def _setup_exec(self, mock_ssh):
        _wire_exec_channel(mock_ssh, _WindowedChannel())

    def test_move_calls_mv(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr()
        self._setup_exec(mock_ssh)

        backend.move("old.txt", "new.txt")
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "mv --" in cmd
        assert "/home/agent-host/workspace/old.txt" in cmd
        assert "/home/agent-host/workspace/new.txt" in cmd

    def test_move_nonexistent_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.side_effect = FileNotFoundError()
        with pytest.raises(FileNotFoundError, match="Source not found"):
            backend.move("ghost.txt", "dst.txt")

    def test_replace_file_rejects_directory_destination(self, remote_backend):
        backend, _mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.side_effect = [
            _make_sftp_attr(is_dir=False),
            _make_sftp_attr(is_dir=True),
        ]

        with pytest.raises(ValueError, match="Destination is a directory"):
            backend.replace_file("old.txt", "existing-dir")

    def test_replace_file_uses_exact_mv_target(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        _wire_exec_channel(mock_ssh, _WindowedChannel())

        backend.replace_file("old.txt", "new.txt")

        assert "mv -T --" in mock_ssh.exec_command.call_args[0][0]

    def test_move_nonzero_exit_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        _wire_exec_channel(mock_ssh, _WindowedChannel(exit_code=1))

        with pytest.raises(OSError, match="Remote move failed with exit code 1"):
            backend.move("old.txt", "new.txt")


class TestRemoteBackendCopy:
    """Tests for RemoteBackend.copy()."""

    def _setup_exec(self, mock_ssh):
        _wire_exec_channel(mock_ssh, _WindowedChannel())

    def test_copy_calls_cp(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False)
        self._setup_exec(mock_ssh)

        backend.copy("src.txt", "dst.txt")
        cmd = mock_ssh.exec_command.call_args[0][0]
        assert "cp -a" in cmd

    def test_copy_nonexistent_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.side_effect = FileNotFoundError()
        with pytest.raises(FileNotFoundError, match="Source not found"):
            backend.copy("ghost.txt", "dst.txt")

    def test_copy_directory_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)
        with pytest.raises(ValueError, match="Cannot copy directory"):
            backend.copy("dir", "dst")


class TestRemoteBackendStat:
    """Tests for RemoteBackend.stat()."""

    def test_stat_file_returns_size(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=False, size=1024)
        size = backend.stat("file.txt")
        assert size == 1024

    def test_stat_nonexistent_returns_zero(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.side_effect = FileNotFoundError()
        assert backend.stat("ghost.txt") == 0

    def test_stat_directory_uses_du(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)

        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(stdout_data=b"4096\t/home/agent-host/workspace/dir\n"),
        )

        size = backend.stat("dir")
        assert size == 4096

    def test_stat_directory_du_parse_error(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_sftp.stat.return_value = _make_sftp_attr(is_dir=True)

        _wire_exec_channel(
            mock_ssh, _WindowedChannel(stdout_data=b"malformed output\n")
        )

        assert backend.stat("dir") == 0


class TestHeavyOpTimeouts:
    """Timeouts now actually bind (Task 1), so heavy ops need explicit
    generous deadlines or big trees would newly fail at the 30s default."""

    @pytest.mark.parametrize(
        "call,expected_timeout",
        [
            (lambda b: b.delete_directory("big"), 300),
            (lambda b: b.copy("a", "b"), 300),
            (lambda b: b.move("a", "b"), 120),
            (lambda b: b.stat("big"), 120),
        ],
    )
    def test_heavy_ops_pass_generous_timeouts(
        self, remote_backend, call, expected_timeout
    ):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Mock stat to return directory for "big" paths, files for others
        def mock_stat(path):
            if "big" in path:
                return _make_sftp_attr(is_dir=True)
            else:
                return _make_sftp_attr(is_dir=False)

        mock_sftp.stat.side_effect = mock_stat
        with (
            patch.object(backend, "_exec", return_value="0\t/ws") as ex,
            patch.object(backend, "_exec_with_status", return_value=("", 0)) as checked,
        ):
            call(backend)
        invoked = checked if checked.called else ex
        assert invoked.call_args.kwargs.get("timeout") == expected_timeout


class TestRemoteBackendExec:
    """Tests for RemoteBackend._exec()."""

    def test_exec_returns_stdout(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        _wire_exec_channel(mock_ssh, _WindowedChannel(stdout_data=b"output data\n"))

        result = backend._exec("echo hello")
        assert result == "output data\n"

    def test_exec_nonzero_exit_still_returns_output(self, remote_backend):
        """Non-zero exit codes are logged but output is still returned."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(
                stdout_data=b"some output\n", stderr_data=b"error msg", exit_code=1
            ),
        )

        result = backend._exec("grep notfound .")
        assert result == "some output\n"

    def test_exec_ssh_error_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        mock_ssh.exec_command.side_effect = socket.error("broken pipe")
        with pytest.raises(WorkspaceUnavailableError, match="SSH command failed"):
            backend._exec("ls")


class TestRemoteBackendTmuxFences:
    """The durable tmux protocol fails closed across owner handoffs."""

    @staticmethod
    def _incarnation_backend(
        token: int = 21, runtime_incarnation: str = _RUNTIME_INCARNATION
    ) -> RemoteBackend:
        backend = RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id="aaaa-bbbb-cccc-dddd",
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=runtime_incarnation,
        )
        backend.set_shell_owner_token(token)
        return backend

    def test_pinned_claim_resource_exec_preserves_plain_command(self, remote_backend):
        backend, _, _ = remote_backend
        with patch.object(backend, "exec_command", return_value="ok") as execute:
            assert (
                backend.exec_claim_resource(
                    "touch /tmp/example", timeout=17, operation="test mutation"
                )
                == "ok"
            )
        execute.assert_called_once_with("touch /tmp/example", timeout=17)

    def test_eager_claim_loads_generation_for_attach_time_resource_mutation(self):
        backend = self._incarnation_backend(token=21)
        generation = "a" * 32

        with (
            patch.object(backend, "_ensure_connected") as ensure_connected,
            patch.object(
                backend,
                "_create_or_observe_tmux_session",
                return_value="existing",
            ) as promote,
            patch.object(
                backend,
                "_read_tmux_session_option",
                return_value=generation,
            ) as read_option,
            patch.object(
                backend,
                "execute_with_secret_stdin",
                return_value=True,
            ) as execute,
        ):
            backend.claim_shell_owner()
            assert backend.execute_claim_resource_with_secret_stdin(
                "printf managed-repository", b"private", timeout=23
            )

        ensure_connected.assert_called_once_with()
        promote.assert_called_once_with()
        read_option.assert_called_once_with("@srw_generation")
        assert backend._shell_generation == generation
        assert "SRW_SHELL_PROCESS_TAG" in execute.call_args.args[0]

    def test_eager_claim_rejects_malformed_process_generation(self):
        backend = self._incarnation_backend(token=21)

        with (
            patch.object(backend, "_ensure_connected"),
            patch.object(
                backend,
                "_create_or_observe_tmux_session",
                return_value="existing",
            ),
            patch.object(
                backend,
                "_read_tmux_session_option",
                return_value="not-a-generation",
            ),
            pytest.raises(
                WorkspaceUnavailableError,
                match="process generation is malformed",
            ),
        ):
            backend.claim_shell_owner()

        assert backend._shell_generation is None

    def test_stateless_resource_fence_is_separate_from_shell_retirement(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(44)
        backend.retire_shell_owner()

        with patch.object(
            backend, "_exec_with_status", return_value=("resource-ok", 0)
        ) as execute:
            assert (
                backend.exec_claim_resource(
                    "touch /cloud/state", timeout=19, operation="cloud mutation"
                )
                == "resource-ok"
            )

        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert "bash -c" in command
        assert '"$_srw_token" = 44 ] || exit 75' in command
        assert '"$_srw_tmux_token" = 44 ] || exit 75' in command
        assert "touch /cloud/state" in command
        assert execute.call_args.kwargs == {"timeout": 19, "retain_tail": True}

    def test_secret_resource_materialization_is_claim_fenced_and_stale_owner_refused(
        self,
    ):
        predecessor = self._incarnation_backend(token=21)
        predecessor._shell_generation = "a" * 32
        predecessor.retire_shell_owner()
        with patch.object(
            predecessor, "execute_with_secret_stdin", return_value=True
        ) as execute:
            assert predecessor.execute_claim_resource_with_secret_stdin(
                "printf new-config", b"private", timeout=23
            )
            guarded = execute.call_args.args[0]
            assert '"$_srw_token" = 21 ] || exit 75' in guarded
            assert "printf new-config" in guarded

            predecessor.retire_claim_resource_owner()
            with pytest.raises(WorkspaceUnavailableError, match="retired"):
                predecessor.execute_claim_resource_with_secret_stdin(
                    "printf stale-config", b"private", timeout=23
                )
            assert execute.call_count == 1

        successor = self._incarnation_backend(token=22)
        successor._shell_generation = "b" * 32
        with patch.object(
            successor, "execute_with_secret_stdin", return_value=True
        ) as execute_successor:
            assert successor.execute_claim_resource_with_secret_stdin(
                "printf successor-config", b"private", timeout=23
            )
        assert '"$_srw_token" = 22 ] || exit 75' in execute_successor.call_args.args[0]

    def test_claim_resource_lock_uses_bash_for_pipefail_scripts(self, tmp_path):
        backend = self._incarnation_backend()
        command = backend._tmux_lock_command(
            "set -euo pipefail\nprintf '__SRW_PIPEFAIL_OK__\\n'",
            shell="bash",
        )

        completed = subprocess.run(
            ["/bin/sh", "-c", command],
            env={**os.environ, "HOME": str(tmp_path)},
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout == "__SRW_PIPEFAIL_OK__\n"

    def test_terminal_claim_resource_uses_bash_for_cleanup_script(self):
        backend = self._incarnation_backend(token=47)
        with patch.object(
            backend, "_exec_with_status", return_value=("terminal-ok", 0)
        ) as execute:
            assert (
                backend.exec_terminal_claim_resource(
                    "set -euo pipefail\nprintf '__SRW_TERMINAL_CLEANUP__\\n'",
                    timeout=23,
                    operation="resident cleanup",
                )
                == "terminal-ok"
            )

        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert "bash -c" in command
        assert "__SRW_TERMINAL_CLEANUP__" in command
        assert execute.call_args.kwargs == {"timeout": 23, "retain_tail": True}

    def test_retired_resource_verification_uses_bash_for_zero_script(self):
        backend = self._incarnation_backend(token=48)
        with patch.object(
            backend, "_exec_with_status", return_value=("zero-ok", 0)
        ) as execute:
            assert (
                backend.verify_terminal_claim_resources_retired(
                    "set -euo pipefail\nprintf '__SRW_RESOURCE_ZERO__\\n'",
                    timeout=29,
                    operation="resident zero proof",
                )
                == "zero-ok"
            )

        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert "bash -c" in command
        assert "__SRW_RESOURCE_ZERO__" in command
        assert '"$_srw_process_tagged" = true' in command
        assert "exit 81" in command
        assert execute.call_args.kwargs == {"timeout": 29, "retain_tail": True}

    def test_retired_resource_verification_closes_its_own_sftp_before_the_proof(
        self,
    ):
        """The proof's own SFTP server is a non-dumpable same-UID sibling.

        OpenSSH's ``sftp-server`` disables tracing on itself, so the workspace
        user cannot read its environment and the read-only zero scan correctly
        refuses (86) beside it. Like the terminate proof in ``shell_cleanup``,
        the verification closes this backend's SFTP channel first and runs over
        the still-active transport.
        """
        backend = self._incarnation_backend(token=48)
        sftp = MagicMock()
        backend._sftp = sftp
        seen_at_exec = []

        def execute(command, **kwargs):
            seen_at_exec.append((backend._sftp, sftp.close.called))
            return "zero-ok", 0

        with (
            patch.object(backend, "_ensure_connected"),
            patch.object(backend, "_exec_with_status", side_effect=execute),
        ):
            backend.verify_terminal_claim_resources_retired(":", 30)

        assert seen_at_exec == [(None, True)]

    def test_tmux_lock_command_defaults_to_sh_and_rejects_other_shells(self):
        backend = self._incarnation_backend()

        command = backend._tmux_lock_command("printf default-shell")

        assert " sh -c " in command
        assert "bash -c" not in command
        with pytest.raises(ValueError, match="must be sh or bash"):
            backend._tmux_lock_command("true", shell="zsh")

    def test_stateless_resource_nonzero_rejects_stale_mutation(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(45)
        with patch.object(backend, "_exec_with_status", return_value=("", 75)):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 75"):
                backend.exec_claim_resource("fusermount3 -u /cloud/home")

    def test_backend_retire_waits_for_admitted_resource_and_blocks_later_calls(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(46)
        entered = threading.Event()
        release = threading.Event()
        retired = threading.Event()
        errors: list[BaseException] = []

        def run_resource(*_args, **_kwargs):
            entered.set()
            assert release.wait(timeout=2)
            return "done", 0

        def mutate():
            try:
                backend.exec_claim_resource("touch /cloud/owned")
            except BaseException as exc:  # pragma: no cover - assertion aid
                errors.append(exc)

        def retire():
            backend.retire()
            retired.set()

        with (
            patch.object(backend, "_exec_with_status", side_effect=run_resource),
            patch.object(backend, "disconnect"),
        ):
            mutation_thread = threading.Thread(target=mutate)
            retire_thread = threading.Thread(target=retire)
            mutation_thread.start()
            assert entered.wait(timeout=2)
            retire_thread.start()
            assert not retired.wait(timeout=0.05)
            release.set()
            mutation_thread.join(timeout=2)
            retire_thread.join(timeout=2)

        assert errors == []
        assert retired.is_set()
        with pytest.raises(WorkspaceUnavailableError, match="claim-resource owner"):
            backend.exec_claim_resource("touch /cloud/stale")

    def test_stateless_owner_token_is_immutable_per_backend(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(10)
        backend.set_shell_owner_token(10)

        with pytest.raises(WorkspaceUnavailableError, match="fresh backend"):
            backend.set_shell_owner_token(11)

        assert backend._shell_owner_token == 10

    def test_stateless_create_command_covers_durable_marker_matrix(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(12)

        with patch.object(
            backend, "_tmux_exec_checked", return_value="existing"
        ) as execute:
            assert backend._stateless_create_or_observe_tmux_session() == "existing"

        command = execute.call_args.args[0]
        assert "$HOME/.srw/tmux" in command
        assert "_srw_load_state" in command
        assert '"$_srw_status" = active' in command
        assert '"$_srw_status" = creating' in command
        # The remaining marker state is the retired tombstone branch and must
        # require a strictly newer claim before creating a new generation.
        assert '-gt "$_srw_token"' in command
        assert "tmux has-session" in command
        assert "exit 79" in command  # active marker + missing/mismatched tmux
        assert "_srw_write_state creating" in command
        assert "_srw_write_state active" in command
        assert _TMUX_GENERATION_OPTION in command

    def test_creating_marker_recovery_resets_partial_tmux_before_create(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(13)

        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            assert backend._stateless_create_or_observe_tmux_session() == "created"

        command = execute.call_args.args[0]
        creating_branch = command.index('"$_srw_status" = creating')
        reset = command.index("tmux kill-session", creating_branch)
        recreate = command.index("tmux new-session", reset)
        assert creating_branch < reset < recreate

    def test_incarnation_fence_binds_stable_backing_and_current_pod(self):
        backend = self._incarnation_backend()

        with patch.object(
            backend, "_tmux_exec_checked", return_value="existing"
        ) as execute:
            assert backend._stateless_create_or_observe_tmux_session() == "existing"

        command = execute.call_args.args[0]
        assert f"3|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|" in command
        assert f"|{_RUNTIME_INCARNATION}|%s|%s|%s" in command
        assert _TMUX_WORKSPACE_GENERATION_OPTION in command
        assert _TMUX_RUNTIME_INCARNATION_OPTION in command
        assert f'"$_srw_runtime_incarnation" = {_RUNTIME_INCARNATION}' in command
        assert f'"$_srw_tmux_runtime_incarnation" = {_RUNTIME_INCARNATION}' in command

    def test_incarnation_creation_stamps_inherited_process_tags(self):
        backend = self._incarnation_backend()
        generation = "c" * 32

        command = backend._stateless_tmux_create_shell("21", generation)

        assert "@srw_process_tag" in command
        assert "SRW_WORKSPACE_PROCESS_TAG" in command
        assert "SRW_SHELL_PROCESS_TAG" in command
        assert backend._workspace_process_tag() in command
        assert backend._shell_process_tag(generation) in command
        new_session = command.index("tmux new-session")
        first_option = command.index("tmux set-option", new_session)
        assert " -e SRW_WORKSPACE_PROCESS_TAG=" in command[new_session:first_option]
        assert " -e SRW_SHELL_PROCESS_TAG=" in command[new_session:first_option]

    @staticmethod
    def _pinned_sandbox_backend() -> RemoteBackend:
        return RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=_RUNTIME_INCARNATION,
            expected_host_key_fingerprint="SHA256:" + "A" * 43,
            workspace_tier="sandbox",
            sudo_action="freeze",
        )

    def test_pinned_sandbox_zero_proof_is_kernel_uid_scoped_not_tag_scoped(self):
        backend = self._pinned_sandbox_backend()

        command = backend._dedicated_workspace_uid_process_zero_shell(terminate=True)

        assert "/proc" in command
        assert "os.geteuid()" in command
        assert 'open(f"/proc/{pid}/stat"' in command
        assert "SIGTERM" in command and "SIGKILL" in command
        assert "SRW_WORKSPACE_PROCESS_TAG" not in command
        assert "SRW_SHELL_PROCESS_TAG" not in command
        assert "raise SystemExit(86)" in command

    def test_pinned_sandbox_uid_zero_kills_tag_cleared_detached_and_forking_writers(
        self, tmp_path
    ):
        """Execute the proof in a disposable PID/user namespace.

        The parent test process is invisible in the namespace, so the cleanup
        can truthfully scan the whole workspace UID without risking unrelated
        developer/CI processes. One writer clears both cooperative tags and
        starts a new session; another forks while the scan begins.
        """

        unshare = shutil.which("unshare")
        if unshare is None:
            pytest.skip("unshare is unavailable")
        backend = self._pinned_sandbox_backend()
        cleanup = backend._dedicated_workspace_uid_process_zero_shell(terminate=True)
        escaped_write = tmp_path / "escaped-after-proof"
        harness = r"""
import os
import subprocess
import sys
import threading
import time

sentinel = os.environ["SRW_UID_ZERO_SENTINEL"]
writer = subprocess.Popen(
    [
        "setsid", "env", "-u", "SRW_WORKSPACE_PROCESS_TAG", "-u",
        "SRW_SHELL_PROCESS_TAG", "sh", "-c",
        'trap "" TERM; sleep 8; printf escaped > "$SRW_UID_ZERO_SENTINEL"; '
        'while :; do sleep 1; done',
    ],
    env={**os.environ, "SRW_UID_ZERO_SENTINEL": sentinel},
)
forker_code = r'''
import os, signal, time
running = True
children = set()
def stop(_sig, _frame):
    global running
    running = False
def reap(_sig=None, _frame=None):
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid <= 0:
            return
        children.discard(pid)
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGCHLD, reap)
while running:
    pid = os.fork()
    if pid == 0:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        time.sleep(0.15)
        os._exit(0)
    children.add(pid)
    time.sleep(0.005)
for pid in list(children):
    try: os.kill(pid, signal.SIGTERM)
    except ProcessLookupError: pass
deadline = time.monotonic() + 1
while children and time.monotonic() < deadline:
    reap()
    time.sleep(0.01)
'''
forker = subprocess.Popen([sys.executable, "-c", forker_code])
for process in (writer, forker):
    threading.Thread(target=process.wait, daemon=True).start()
time.sleep(0.1)
completed = subprocess.run(
    ["bash", "-c", os.environ["SRW_UID_ZERO_COMMAND"]],
    text=True,
    capture_output=True,
    timeout=15,
    check=False,
)
if completed.returncode:
    print(completed.stderr, file=sys.stderr)
    raise SystemExit(completed.returncode)
for process in (writer, forker):
    deadline = time.monotonic() + 2
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.02)
    if process.poll() is None:
        raise SystemExit(91)
time.sleep(1.1)
if os.path.exists(sentinel):
    raise SystemExit(92)
"""
        completed = subprocess.run(
            [
                unshare,
                "--user",
                "--map-root-user",
                "--mount",
                "--pid",
                "--fork",
                "--mount-proc",
                sys.executable,
                "-c",
                harness,
            ],
            env={
                **os.environ,
                "SRW_UID_ZERO_COMMAND": cleanup,
                "SRW_UID_ZERO_SENTINEL": str(escaped_write),
            },
            text=True,
            capture_output=True,
            timeout=25,
            check=False,
        )
        if completed.returncode != 0 and "Operation not permitted" in completed.stderr:
            pytest.skip("unprivileged PID/user namespaces are unavailable")
        assert completed.returncode == 0, completed.stderr
        assert not escaped_write.exists()

    def test_pinned_sandbox_strict_cleanup_mints_only_workspace_zero_proof(self):
        backend = self._pinned_sandbox_backend()

        with patch.object(backend, "_tmux_exec_checked") as execute:
            assert backend.protected_workspace_zero_cleanup_strict() == (
                "workspace_process_zero_v1"
            )

        command = execute.call_args.args[0]
        assert "/proc" in command
        assert "SRW_WORKSPACE_PROCESS_TAG" in command  # tmux ownership check
        assert "os.geteuid()" in command  # tag-cleared descendants still covered
        assert backend.terminal_local_quiescence_protocol == (
            "workspace_process_zero_v1"
        )

    def test_vm_cannot_mint_sandbox_process_zero_proof(self):
        backend = RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=_RUNTIME_INCARNATION,
            expected_host_key_fingerprint="SHA256:" + "A" * 43,
            workspace_tier="vm",
            sudo_action="allow",
        )

        with (
            patch.object(backend, "_tmux_exec_checked") as execute,
            pytest.raises(WorkspaceUnavailableError, match="sandbox authority"),
        ):
            backend.protected_workspace_zero_cleanup_strict()
        execute.assert_not_called()

    def test_worker_process_tag_uses_exact_job_workspace_owner(self):
        job_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        backend = RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id=job_id,
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=_RUNTIME_INCARNATION,
            workspace_owner_kind="job",
            workspace_owner_id=job_id,
        )

        assert backend._workspace_process_tag() == (
            f"v1:job:{job_id}:{_RUNTIME_INCARNATION}"
        )
        assert backend.shared_workspace is False

    def test_shared_child_terminal_cleanup_is_exact_shell_scoped(self):
        child_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        parent_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        backend = RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id=child_id,
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=_RUNTIME_INCARNATION,
            workspace_owner_kind="job",
            workspace_owner_id=parent_id,
        )
        backend.set_shell_owner_token(31)

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()

        command = execute.call_args.args[0]
        assert backend._workspace_process_tag() == (
            f"v1:job:{parent_id}:{_RUNTIME_INCARNATION}"
        )
        assert backend.shared_workspace is True
        assert "python3 - SRW_SHELL_PROCESS_TAG" in command
        assert "python3 - SRW_WORKSPACE_PROCESS_TAG" not in command
        assert f"v1:{child_id}:{_WORKSPACE_GENERATION}:" in command
        assert "_srw_retire_incarnation=absent" not in command

    def test_shared_child_process_zero_leaves_parent_tagged_process_alive(self):
        """Run in a private PID namespace, where a zero proof is exact.

        On a shared host the whole-``/proc`` scan legitimately refuses (86)
        beside same-UID processes it cannot inspect, so a host run could only
        ever show the refusal. See ``tests/_process_zero_namespace.py``.
        """

        child_id = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        parent_id = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        backend = RemoteBackend(
            host="workspace.test",
            workspace_path="/home/agent-host/workspace",
            job_id=child_id,
            workspace_generation=_WORKSPACE_GENERATION,
            runtime_incarnation=_RUNTIME_INCARNATION,
            workspace_owner_kind="job",
            workspace_owner_id=parent_id,
        )
        generation = "c" * 32
        shell_tag = backend._shell_process_tag(generation)
        workspace_tag = backend._workspace_process_tag()
        command = (
            f"_srw_generation={generation}\n"
            + backend._stateless_terminal_process_zero_shell(terminate=True)
        )
        try:
            result = run_process_zero_scenario(
                command=command,
                tag_env="SRW_SHELL_PROCESS_TAG",
                fixtures=[
                    {
                        "name": "child",
                        "kind": "term_logger",
                        "env": {
                            "SRW_WORKSPACE_PROCESS_TAG": workspace_tag,
                            "SRW_SHELL_PROCESS_TAG": shell_tag,
                        },
                    },
                    {
                        "name": "parent",
                        "kind": "term_logger",
                        "env": {"SRW_WORKSPACE_PROCESS_TAG": workspace_tag},
                    },
                ],
            )
        except LookupError as exc:
            pytest.skip(str(exc))

        scan = result["scans"][0]
        assert scan["returncode"] == 0, scan
        child, parent = result["after"]["child"], result["after"]["parent"]
        assert not child["executing"] and child["log"] == ["TERM"]
        assert parent["executing"] and parent["log"] == []

    def test_terminal_process_zero_kills_disowned_tagged_child_and_excludes_ancestors(
        self,
    ):
        """The scan carries the tag itself, as it does inside the workspace.

        A zero proof (exit 0) is only possible if the scan excluded its own
        tagged ancestry and still retired the disowned, session-leading child.
        """

        backend = self._incarnation_backend()
        tag = backend._workspace_process_tag()
        command = backend._stateless_workspace_process_zero_shell(terminate=True)
        assert "\x00" not in command
        try:
            result = run_process_zero_scenario(
                command=command,
                scan_env={"SRW_WORKSPACE_PROCESS_TAG": tag},
                fixtures=[
                    {
                        "name": "child",
                        "kind": "term_logger",
                        "env": {"SRW_WORKSPACE_PROCESS_TAG": tag},
                    }
                ],
            )
        except LookupError as exc:
            pytest.skip(str(exc))

        scan = result["scans"][0]
        assert scan["returncode"] == 0, scan
        assert result["before"]["child"]["executing"]
        child = result["after"]["child"]
        assert not child["executing"] and child["log"] == ["TERM"]

    def test_replacement_runtime_supersedes_stale_marker_only_without_live_tmux(self):
        backend = self._incarnation_backend(token=22)

        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            assert backend._stateless_create_or_observe_tmux_session() == "created"

        command = execute.call_args.args[0]
        stale_branch = command.rindex('elif [ "$_srw_marker" = present ]; then')
        live_conflict = command.index("tmux has-session", stale_branch)
        reject = command.index("exit 80", live_conflict)
        recreate = command.index("tmux new-session", reject)
        assert stale_branch < live_conflict < reject < recreate

    @staticmethod
    def _run_generated_tmux_command_without_session(
        backend: RemoteBackend, command: str, tmp_path: Path
    ) -> subprocess.CompletedProcess[str]:
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        fake_tmux = fake_bin / "tmux"
        fake_tmux.write_text(
            '#!/bin/sh\nif [ "$1" = "has-session" ]; then exit 1; fi\nexit 0\n'
        )
        fake_tmux.chmod(0o755)
        env = dict(os.environ)
        env["HOME"] = str(tmp_path)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        return subprocess.run(
            ["bash", "-c", command],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_replacement_runtime_rewrites_older_marker_on_shared_backing(
        self, tmp_path
    ):
        old_runtime = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        backend = self._incarnation_backend(token=22)
        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            backend._stateless_create_or_observe_tmux_session()
        command = execute.call_args.args[0]
        state_dir = tmp_path / ".srw" / "tmux"
        state_dir.mkdir(parents=True)
        state_file = state_dir / backend._tmux_state_filename
        state_file.write_text(
            f"2|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|"
            f"{old_runtime}|active|21|{'a' * 32}\n"
        )

        completed = self._run_generated_tmux_command_without_session(
            backend, command, tmp_path
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout == "created"
        fields = state_file.read_text().strip().split("|")
        assert fields[:6] == [
            "3",
            backend._tmux_owner_digest,
            _WORKSPACE_GENERATION,
            _RUNTIME_INCARNATION,
            "active",
            "22",
        ]
        assert re.fullmatch(r"[0-9a-f]{32}", fields[6])

    def test_stale_runtime_cannot_overwrite_newer_claim_marker(self, tmp_path):
        old_runtime = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        backend = self._incarnation_backend(token=21, runtime_incarnation=old_runtime)
        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            backend._stateless_create_or_observe_tmux_session()
        command = execute.call_args.args[0]
        state_dir = tmp_path / ".srw" / "tmux"
        state_dir.mkdir(parents=True)
        state_file = state_dir / backend._tmux_state_filename
        successor_state = (
            f"2|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|"
            f"{_RUNTIME_INCARNATION}|active|22|{'b' * 32}\n"
        )
        state_file.write_text(successor_state)

        completed = self._run_generated_tmux_command_without_session(
            backend, command, tmp_path
        )

        assert completed.returncode == 75
        assert state_file.read_text() == successor_state

    def test_stale_runtime_cleanup_cannot_retire_newer_claim_marker(self, tmp_path):
        old_runtime = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        backend = self._incarnation_backend(token=21, runtime_incarnation=old_runtime)
        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()
        command = execute.call_args.args[0]
        state_dir = tmp_path / ".srw" / "tmux"
        state_dir.mkdir(parents=True)
        state_file = state_dir / backend._tmux_state_filename
        successor_state = (
            f"2|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|"
            f"{_RUNTIME_INCARNATION}|active|22|{'b' * 32}\n"
        )
        state_file.write_text(successor_state)

        completed = self._run_generated_tmux_command_without_session(
            backend, command, tmp_path
        )

        # Protocol-v2 successor records predate mandatory process tags and are
        # refused before the later token comparison.
        assert completed.returncode == 81
        assert state_file.read_text() == successor_state

    @pytest.mark.parametrize(
        "legacy_state",
        [
            lambda backend: (f"1|{backend._tmux_owner_digest}|active|20|{'a' * 32}\n"),
            lambda backend: (
                f"2|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|"
                f"{_RUNTIME_INCARNATION}|active|20|{'a' * 32}\n"
            ),
            lambda backend: (
                f"2|{backend._tmux_owner_digest}|{_WORKSPACE_GENERATION}|"
                f"{_RUNTIME_INCARNATION}|retired|20|{'a' * 32}\n"
            ),
        ],
        ids=["v1-active", "v2-active", "v2-retired"],
    )
    def test_pre_process_tag_records_exit_81_without_mutation(
        self, tmp_path, legacy_state
    ):
        backend = self._incarnation_backend(token=23)
        with patch.object(
            backend, "_tmux_exec_checked", return_value="existing"
        ) as execute:
            backend._stateless_create_or_observe_tmux_session()
        command = execute.call_args.args[0]
        state_dir = tmp_path / ".srw" / "tmux"
        state_dir.mkdir(parents=True)
        state_file = state_dir / backend._tmux_state_filename
        original = legacy_state(backend)
        state_file.write_text(original)

        completed = self._run_generated_tmux_command_without_session(
            backend, command, tmp_path
        )

        assert completed.returncode == 81
        assert state_file.read_text() == original

    def test_stateless_cleanup_writes_tombstone_before_kill(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(14)

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()

        command = execute.call_args.args[0]
        tombstone = command.index("_srw_write_state retired")
        kill = command.index("tmux kill-session", tombstone)
        assert tombstone < kill
        assert "$HOME/.srw/tmux" in command
        assert '"$_srw_token" -le 14' in command
        assert _TMUX_GENERATION_OPTION in command
        assert execute.call_args.kwargs["allow_shell_retired"] is True
        assert execute.call_args.kwargs["close_sftp"] is True

    def test_terminal_tmux_proof_closes_own_sftp_before_exec(self):
        backend = self._incarnation_backend(token=14)
        sftp = MagicMock()
        backend._sftp = sftp
        events = []
        sftp.close.side_effect = lambda: events.append("sftp.close")

        with (
            patch.object(
                backend,
                "_ensure_connected",
                side_effect=lambda: events.append("ensure"),
            ) as ensure_connected,
            patch.object(
                backend,
                "_exec_with_status",
                side_effect=lambda *_args, **_kwargs: (
                    events.append("exec") or ("retired", 0)
                ),
            ) as execute,
        ):
            assert (
                backend._tmux_exec_checked(
                    "terminal-proof",
                    operation="terminal proof",
                    allow_shell_retired=True,
                    close_sftp=True,
                )
                == "retired"
            )

        ensure_connected.assert_called_once_with()
        sftp.close.assert_called_once_with()
        assert backend._sftp is None
        execute.assert_called_once_with("terminal-proof", retain_tail=True)
        assert events == ["ensure", "sftp.close", "exec"]

    def test_incarnation_cleanup_is_strict_and_checks_both_authorities(self):
        backend = self._incarnation_backend(token=24)

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()

        command = execute.call_args.args[0]
        assert _WORKSPACE_GENERATION in command
        assert _RUNTIME_INCARNATION in command
        assert _TMUX_WORKSPACE_GENERATION_OPTION in command
        assert _TMUX_RUNTIME_INCARNATION_OPTION in command
        assert command.index("_srw_write_state retired") < command.index(
            "tmux kill-session"
        )

    def test_incarnation_cleanup_surfaces_missing_remote_ack(self):
        backend = self._incarnation_backend(token=25)
        backend._shell_initialized = True

        with (
            patch.object(
                backend,
                "_tmux_exec_checked",
                side_effect=WorkspaceUnavailableError("response lost"),
            ),
            pytest.raises(
                WorkspaceUnavailableError,
                match="terminal retirement was not acknowledged",
            ),
        ):
            backend.shell_cleanup()

        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_exact_tmux_targets_do_not_change_raw_create_name(self, remote_backend):
        backend, _, _ = remote_backend
        assert backend._tmux_target() == "=agent_aaaa-bbbb-cc:"
        assert backend._tmux_target("default") == "=agent_aaaa-bbbb-cc:=default"

        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            backend._create_or_observe_tmux_session()

        command = execute.call_args.args[0]
        assert "tmux new-session -d -s agent_aaaa-bbbb-cc" in command
        assert "new-session -d -s '=agent_aaaa-bbbb-cc:'" not in command

    def test_checked_tmux_nonzero_clears_untrusted_local_state(self, remote_backend):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(
            backend, "_exec_with_status", return_value=("partial output", 75)
        ):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 75"):
                backend._tmux_exec_checked("tmux list-windows", operation="probe")

        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_checked_tmux_requests_bounded_tail_retention(self, remote_backend):
        backend, _, _ = remote_backend
        with patch.object(
            backend, "_exec_with_status", return_value=("tail", 0)
        ) as execute:
            assert (
                backend._tmux_exec_checked("tmux capture-pane", operation="capture")
                == "tail"
            )
        execute.assert_called_once_with("tmux capture-pane", retain_tail=True)

    def test_capture_joins_only_display_wrapped_lines(self, remote_backend):
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(
            backend,
            "_tmux_exec_checked",
            return_value="first logical line\nsecond logical line\n",
        ) as execute:
            assert backend._tmux_capture("default") == [
                "first logical line",
                "second logical line",
            ]

        command = execute.call_args.args[0]
        assert "capture-pane -J " in command
        assert f"-S -{backend._scrollback_limit}" in command

    def test_capture_prunes_only_proven_gone_pane_and_keeps_shell_live(
        self, remote_backend, caplog
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(17)
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["verify-spec-lock"] = _RemoteTab("verify-spec-lock", pane_id="%2")
        commands = []

        def execute(command, **_kwargs):
            commands.append(command)
            if command.startswith("tmux capture-pane"):
                if "%2" in command:
                    return "", 1
                return "surviving pane\n", 0
            if command.startswith("tmux has-session"):
                return "", 0
            if command.startswith("tmux list-panes"):
                return "", 1
            return "", 0

        with (
            patch.object(backend, "_exec_with_status", side_effect=execute),
            caplog.at_level(logging.WARNING),
        ):
            with pytest.raises(KeyError, match="no longer exists"):
                backend._tmux_capture("verify-spec-lock")
            assert backend._tmux_capture("default") == ["surviving pane"]

        assert backend._shell_initialized is True
        assert list(backend._tabs) == ["default"]
        assert "pane_lost_at_handoff" in caplog.text
        assert "tab=verify-spec-lock" in caplog.text
        assert "shell_owner_token=17" in caplog.text
        cleanup = next(command for command in commands if command.startswith("umask"))
        assert '"$_srw_tmux_token" = 17 ] || exit 75' in cleanup
        assert "tmux kill-window" in cleanup
        assert cleanup.count("set-option -w -u") == 6
        for option in (
            "@srw_tab_type",
            "@srw_pending_sentinel",
            "@srw_pending_since",
            "@srw_pane_id",
            "@srw_window_prompt_token",
            "@srw_window_setup_state",
        ):
            assert option in cleanup

    @pytest.mark.parametrize("enter", (False, True))
    def test_key_send_prunes_closed_tab_without_losing_surviving_shell(
        self, remote_backend, enter
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(17)
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["closed"] = _RemoteTab("closed", pane_id="%2")
        commands = []

        def execute(command, **_kwargs):
            commands.append(command)
            if "tmux send-keys -t %2" in command:
                return "", 1
            if command.startswith("tmux has-session"):
                return "", 0
            if command.startswith("tmux list-panes"):
                return "", 1
            return "", 0

        with patch.object(backend, "_exec_with_status", side_effect=execute):
            with pytest.raises(KeyError, match="Tab 'closed' no longer exists"):
                backend._tmux_send_keys("closed", "C-c", enter=enter)
            assert backend.shell_send("default", "C-c", enter=False) == (
                "Sent to 'default'"
            )

        assert backend._shell_initialized is True
        assert list(backend._tabs) == ["default"]
        cleanup = next(command for command in commands if "tmux kill-window" in command)
        assert '"$_srw_tmux_token" = 17 ] || exit 75' in cleanup

    @pytest.mark.parametrize("exit_code", (75, 78, 79, 80, 81))
    def test_key_send_fence_failure_cannot_be_downgraded_to_tab_loss(
        self, remote_backend, exit_code
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(17)
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["closed"] = _RemoteTab("closed", pane_id="%2")

        with (
            patch.object(
                backend, "_exec_with_status", return_value=("", exit_code)
            ) as execute,
            patch.object(backend, "_tmux_pane_gone") as probe,
        ):
            with pytest.raises(
                WorkspaceUnavailableError, match=f"exit code {exit_code}"
            ):
                backend.shell_send("closed", "C-c", enter=False)

        execute.assert_called_once()
        probe.assert_not_called()
        assert backend._shell_initialized is False
        assert backend._tabs == {}

    @pytest.mark.parametrize("failure", ("session", "transport", "pane_present"))
    def test_key_send_requires_conclusive_pane_absence(self, remote_backend, failure):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["closed"] = _RemoteTab("closed", pane_id="%2")

        def execute(command, **_kwargs):
            if "tmux send-keys" in command:
                return "", 1
            if command.startswith("tmux has-session"):
                if failure == "transport":
                    raise WorkspaceUnavailableError("SSH command failed")
                return "", 1 if failure == "session" else 0
            if command.startswith("tmux list-panes"):
                return "%2\n", 0
            raise AssertionError("Unexpected remote command")

        with patch.object(backend, "_exec_with_status", side_effect=execute):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 1"):
                backend.shell_send("closed", "C-c", enter=False)

        assert backend._shell_initialized is False
        assert backend._tabs == {}

    @pytest.mark.parametrize("probe_failure", ("tmux", "transport"))
    def test_capture_probe_server_failure_keeps_workspace_error_semantics(
        self, remote_backend, probe_failure
    ):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["verify-spec-lock"] = _RemoteTab("verify-spec-lock", pane_id="%2")

        def execute(command, **_kwargs):
            if command.startswith("tmux capture-pane"):
                return "", 1
            if command.startswith("tmux has-session"):
                if probe_failure == "transport":
                    raise WorkspaceUnavailableError("SSH command failed")
                return "", 1
            raise AssertionError(f"Unexpected command: {command}")

        with patch.object(backend, "_exec_with_status", side_effect=execute):
            with pytest.raises(
                WorkspaceUnavailableError,
                match="capture pane verify-spec-lock failed with exit code 1",
            ):
                backend._tmux_capture("verify-spec-lock")

        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_capture_probe_does_not_prune_pane_that_is_still_listed(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        backend._tabs["verify-spec-lock"] = _RemoteTab("verify-spec-lock", pane_id="%2")

        def execute(command, **_kwargs):
            if command.startswith("tmux capture-pane"):
                return "", 1
            if command.startswith("tmux has-session"):
                return "", 0
            if command.startswith("tmux list-panes"):
                return "%2\n", 0
            raise AssertionError(f"Unexpected command: {command}")

        with patch.object(backend, "_exec_with_status", side_effect=execute):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 1"):
                backend._tmux_capture("verify-spec-lock")

        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_reattach_prune_reports_loss_once_before_tab_can_recreate(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(18)
        backend._shell_protocol_current = True
        metadata = "\n".join(
            (
                _tmux_window_row("default", "shell", pane_id="%1"),
                _tmux_window_row(
                    "verify-spec-lock",
                    "shell",
                    pane_id="%2",
                    stored_pane_id="%2",
                ),
            )
        )

        def execute(command, **_kwargs):
            if command.startswith("tmux list-windows"):
                return metadata, 0
            if command.startswith("tmux capture-pane"):
                if "%2" in command:
                    return "", 1
                return "$\n", 0
            if command.startswith("tmux has-session"):
                return "", 0
            if command.startswith("tmux list-panes"):
                return "", 1
            return "", 0

        with patch.object(backend, "_exec_with_status", side_effect=execute):
            backend._rehydrate_tabs()

        assert list(backend._tabs) == ["default"]
        assert backend._lost_tab_notices == {"verify-spec-lock"}
        backend._shell_initialized = True
        with pytest.raises(KeyError, match="verify-spec-lock.*no longer exists"):
            backend.shell_run("echo must-not-run", tab_name="verify-spec-lock")

        with patch.object(backend, "shell_open_tab") as open_tab:
            backend.shell_ensure_tab("verify-spec-lock")
        open_tab.assert_called_once_with("verify-spec-lock")

    def test_full_owner_id_mismatch_refuses_truncated_name_collision(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        # Both IDs produce the same deterministic 12-character tmux name.
        assert backend._session_name == "agent_aaaa-bbbb-cc"
        with (
            patch.object(
                backend,
                "_read_tmux_session_option",
                return_value="aaaa-bbbb-cccc-successor",
            ),
            patch.object(backend, "_set_tmux_session_option") as persist,
        ):
            with pytest.raises(WorkspaceUnavailableError, match="owner identity"):
                backend._attest_tmux_owner()
        persist.assert_not_called()

    def test_owner_token_promotion_is_monotonic_and_locked(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(42)

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend._promote_tmux_owner_token()

        execute.assert_called_once()
        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert _TMUX_OWNER_TOKEN_OPTION in command
        assert '"$_srw_token" = 42 ] || exit 75' in command
        assert '"$_srw_tmux_token" = 42 ] || exit 75' in command
        assert "exit 75" in command
        assert "@srw_generation" in command

    def test_stale_owner_token_mutation_is_rejected_by_checked_status(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(7)
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(
            backend, "_exec_with_status", return_value=("", 75)
        ) as execute:
            with pytest.raises(WorkspaceUnavailableError, match="exit code 75"):
                backend._tmux_mutate_checked(
                    "tmux kill-window -t %1", operation="stale mutation"
                )

        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert _TMUX_OWNER_TOKEN_OPTION in command
        assert "= 7 ] || exit 75" in command
        assert backend._tabs == {}

    def test_atomic_reserve_and_send_is_one_locked_fenced_remote_exec(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(9)
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend._reserve_and_send_shell_command(
                "default",
                expected=None,
                sentinel="__DONE_0123456789ab__",
                command="printf 'hello world'",
            )

        execute.assert_called_once()
        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert _TMUX_OWNER_TOKEN_OPTION in command
        assert "= 9 ] || exit 75" in command
        assert _TMUX_PENDING_SENTINEL_OPTION in command
        assert '"$_srw_pending" != ' in command
        assert "exit 74" in command
        # The guard is stamped, and only a stamped-stale guard may be aged out.
        assert _TMUX_PENDING_SINCE_OPTION in command
        assert f"-ge {_PENDING_GUARD_STALE_SECONDS} ] || exit 74" in command
        assert "__DONE_0123456789ab__" in command
        assert command.count("tmux send-keys") == 2
        assert "%17" in command
        assert backend._tabs["default"].pending_sentinel == "__DONE_0123456789ab__"

    def test_clear_pending_compares_expected_sentinel_before_mutation(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        tab = _RemoteTab("default", pane_id="%1")
        tab.pending_sentinel = "__DONE_bbbbbbbbbbbb__"
        backend._tabs["default"] = tab

        with patch.object(
            backend,
            "_tmux_mutate_checked",
            side_effect=WorkspaceUnavailableError("CAS mismatch"),
        ) as mutate:
            with pytest.raises(WorkspaceUnavailableError, match="CAS mismatch"):
                backend._clear_tab_pending("default", "__DONE_aaaaaaaaaaaa__")

        command = mutate.call_args.args[0]
        assert '"$_srw_pending" = __DONE_aaaaaaaaaaaa__' in command
        assert "|| exit 74" in command
        # Request A did not locally clear the newer request B either.
        assert tab.pending_sentinel == "__DONE_bbbbbbbbbbbb__"

    def test_async_shell_send_installs_unique_completion_guard(self, remote_backend):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(backend, "_tmux_mutate_checked") as mutate:
            result = backend.shell_send("default", "npm run dev", enter=True)

        assert result == "Sent to 'default'"
        mutate.assert_called_once()
        command = mutate.call_args.args[0]
        guard = backend._tabs["default"].pending_sentinel
        assert guard is not None
        assert re.fullmatch(r"__DONE_[0-9a-f]{12}__", guard)
        assert guard in command
        assert "npm run dev" in command
        assert command.count("tmux send-keys") == 2


class TestPendingGuardStaleAging:
    """The durable pane guard ages out instead of refusing forever.

    These tests execute the generated locked mutation for real (bash + flock)
    against a stateful fake tmux, so the CAS/aging decision is proven in the
    shell where it runs, not merely present in the generated text.
    """

    _FAKE_TMUX = """#!/usr/bin/env python3
import os
import sys

state = os.environ["FAKE_TMUX_STATE"]
args = sys.argv[1:]


def path(option):
    return os.path.join(state, option.lstrip("@"))


def read(option):
    try:
        with open(path(option)) as fh:
            return fh.read()
    except FileNotFoundError:
        return ""


if not args:
    sys.exit(0)
cmd, rest = args[0], args[1:]
if cmd == "display-message":
    fmt = rest[-1]
    for option in ("@srw_pending_sentinel", "@srw_pending_since"):
        if option in fmt:
            print(read(option))
            break
    else:
        print("")
elif cmd == "set-option":
    positional = []
    unset = False
    i = 0
    while i < len(rest):
        arg = rest[i]
        if arg == "-u":
            unset = True
        elif arg == "-t":
            i += 1  # consume the target argument
        elif not arg.startswith("-"):
            positional.append(arg)
        i += 1
    option = positional[0]
    if unset:
        try:
            os.remove(path(option))
        except FileNotFoundError:
            pass
    else:
        with open(path(option), "w") as fh:
            fh.write(positional[1] if len(positional) > 1 else "")
elif cmd == "send-keys":
    with open(os.path.join(state, "send_keys.log"), "a") as fh:
        fh.write(" ".join(rest) + chr(10))
sys.exit(0)
"""

    def _local_exec_env(self, tmp_path):
        """Return (state_dir, side_effect) executing exec'd shell locally."""
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir(exist_ok=True)
        fake_tmux = fake_bin / "tmux"
        fake_tmux.write_text(self._FAKE_TMUX)
        fake_tmux.chmod(0o755)
        state = tmp_path / "tmux-state"
        state.mkdir(exist_ok=True)
        env = dict(os.environ)
        env["HOME"] = str(tmp_path)
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["FAKE_TMUX_STATE"] = str(state)

        def run_locally(command, timeout=30, *, retain_tail=False):
            completed = subprocess.run(
                ["bash", "-c", command],
                env=env,
                text=True,
                capture_output=True,
                timeout=15,
                check=False,
            )
            assert completed.returncode != 127, completed.stderr
            return completed.stdout, completed.returncode

        return state, run_locally

    @staticmethod
    def _seed_guard(state, value, since=None):
        (state / "srw_pending_sentinel").write_text(value)
        if since is not None:
            (state / "srw_pending_since").write_text(str(int(since)))

    def test_fresh_foreign_guard_still_refuses_with_exit_74(
        self, remote_backend, tmp_path
    ):
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
        state, run_locally = self._local_exec_env(tmp_path)
        self._seed_guard(state, "__DONE_aaaaaaaaaaaa__", since=time.time() - 30)

        with patch.object(backend, "_exec_with_status", side_effect=run_locally):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 74"):
                backend._reserve_and_send_shell_command(
                    "default",
                    expected=None,
                    sentinel="__DONE_bbbbbbbbbbbb__",
                    command="git status",
                )

        assert (state / "srw_pending_sentinel").read_text() == "__DONE_aaaaaaaaaaaa__"
        assert not (state / "send_keys.log").exists()

    def test_second_reserve_on_freshly_guarded_pane_still_collides(
        self, remote_backend, tmp_path
    ):
        """The CAS still prevents a genuinely concurrent second command."""
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
        state, run_locally = self._local_exec_env(tmp_path)

        with patch.object(backend, "_exec_with_status", side_effect=run_locally):
            backend._reserve_and_send_shell_command(
                "default",
                expected=None,
                sentinel="__DONE_aaaaaaaaaaaa__",
                command="sleep 600",
            )
            assert (
                state / "srw_pending_sentinel"
            ).read_text() == "__DONE_aaaaaaaaaaaa__"
            # The winning reservation stamped its guard with the pane clock.
            assert (state / "srw_pending_since").read_text().isdigit()
            with pytest.raises(WorkspaceUnavailableError, match="exit code 74"):
                backend._reserve_and_send_shell_command(
                    "default",
                    expected=None,
                    sentinel="__DONE_bbbbbbbbbbbb__",
                    command="git status",
                )

        assert (state / "srw_pending_sentinel").read_text() == "__DONE_aaaaaaaaaaaa__"
        log = (state / "send_keys.log").read_text()
        assert "sleep 600" in log
        assert "git status" not in log

    def test_stale_guard_is_cleared_reserve_proceeds_and_warns(
        self, remote_backend, tmp_path, caplog
    ):
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
        state, run_locally = self._local_exec_env(tmp_path)
        self._seed_guard(
            state,
            "__DONE_aaaaaaaaaaaa__",
            since=time.time() - _PENDING_GUARD_STALE_SECONDS - 60,
        )

        with patch.object(backend, "_exec_with_status", side_effect=run_locally):
            with caplog.at_level(
                logging.WARNING, logger="shared.runtime.core.backends.remote"
            ):
                backend._reserve_and_send_shell_command(
                    "default",
                    expected=None,
                    sentinel="__DONE_bbbbbbbbbbbb__",
                    command="git status",
                )

        assert (state / "srw_pending_sentinel").read_text() == "__DONE_bbbbbbbbbbbb__"
        assert "git status" in (state / "send_keys.log").read_text()
        assert backend._tabs["default"].pending_sentinel == "__DONE_bbbbbbbbbbbb__"
        warning = next(
            record
            for record in caplog.records
            if "stale" in record.getMessage().lower()
        )
        assert "__DONE_aaaaaaaaaaaa__" in warning.getMessage()
        assert re.search(r"age_seconds=\d+", warning.getMessage())

    def test_legacy_unstamped_guard_gets_one_full_bound_then_ages_out(
        self, remote_backend, tmp_path
    ):
        """A guard written by pre-aging code has no stamp: the first refusal
        stamps it 'now' (it may be seconds old), and only after a full bound
        does a reserve reclaim the pane."""
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
        state, run_locally = self._local_exec_env(tmp_path)
        self._seed_guard(state, "__DONE_aaaaaaaaaaaa__")

        with patch.object(backend, "_exec_with_status", side_effect=run_locally):
            with pytest.raises(WorkspaceUnavailableError, match="exit code 74"):
                backend._reserve_and_send_shell_command(
                    "default",
                    expected=None,
                    sentinel="__DONE_bbbbbbbbbbbb__",
                    command="git status",
                )
            # The refusal stamped the legacy guard so it can age out.
            assert (state / "srw_pending_since").read_text().isdigit()
            assert not (state / "send_keys.log").exists()

            # Bound elapses; the CAS failure cleared the local registry, as a
            # reattaching claimant would rebuild it.
            (state / "srw_pending_since").write_text(
                str(int(time.time()) - _PENDING_GUARD_STALE_SECONDS - 5)
            )
            backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
            backend._reserve_and_send_shell_command(
                "default",
                expected=None,
                sentinel="__DONE_cccccccccccc__",
                command="git push",
            )

        assert (state / "srw_pending_sentinel").read_text() == "__DONE_cccccccccccc__"
        assert "git push" in (state / "send_keys.log").read_text()

    def test_clear_tab_pending_drops_the_freshness_stamp(
        self, remote_backend, tmp_path
    ):
        """A cleared guard must not leave a stamp behind: a later guard from
        pre-aging code would inherit it and look instantly stale."""
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%17")
        state, run_locally = self._local_exec_env(tmp_path)

        with patch.object(backend, "_exec_with_status", side_effect=run_locally):
            backend._reserve_and_send_shell_command(
                "default",
                expected=None,
                sentinel="__DONE_aaaaaaaaaaaa__",
                command="git fetch",
            )
            backend._clear_tab_pending("default", "__DONE_aaaaaaaaaaaa__")

        assert not (state / "srw_pending_sentinel").exists()
        assert not (state / "srw_pending_since").exists()
        assert backend._tabs["default"].pending_sentinel is None


class TestRemoteBackendCheckBlocked:
    """Tests for RemoteBackend._check_blocked()."""

    def test_blocked_command_detected(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._check_blocked("reboot -f")
        assert result is not None
        assert "reboot" in result

    def test_allowed_command_returns_none(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._check_blocked("ls -la")
        assert result is None

    def test_empty_command_returns_none(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._check_blocked("")
        assert result is None

    def test_shutdown_blocked(self, remote_backend):
        backend, _, _ = remote_backend
        result = backend._check_blocked("shutdown now")
        assert result is not None

    def test_custom_blocked_commands(self):
        backend = RemoteBackend(
            host="host", workspace_path="/ws", blocked_commands=["rm"]
        )
        assert backend._check_blocked("rm -rf /") is not None
        assert backend._check_blocked("ls -la") is None
        assert backend._check_blocked("reboot") is None  # not in custom list

    def test_sudo_freeze_returns_sentinel(self):
        """sudo_action='freeze' (default) returns SUDO_FREEZE_SENTINEL."""
        from agent.tools.shell.shell_manager import SUDO_FREEZE_SENTINEL

        backend = RemoteBackend(host="host", workspace_path="/ws", sudo_action="freeze")
        result = backend._check_blocked("sudo apt-get install -y libxml2-dev")
        assert result == SUDO_FREEZE_SENTINEL

    def test_sudo_allow_passes_through(self):
        """sudo_action='allow' returns None (VM-backed agents)."""
        backend = RemoteBackend(host="host", workspace_path="/ws", sudo_action="allow")
        result = backend._check_blocked("sudo apt-get install -y libxml2-dev")
        assert result is None

    def test_sudo_block_returns_error(self):
        """sudo_action='block' returns an error message."""
        backend = RemoteBackend(host="host", workspace_path="/ws", sudo_action="block")
        result = backend._check_blocked("sudo ls")
        assert result is not None
        assert "blocked" in result.lower()

    def test_sudo_freeze_is_default(self):
        """Default sudo_action is 'freeze'."""
        backend = RemoteBackend(host="host", workspace_path="/ws")
        assert backend._sudo_action == "freeze"


class TestRemoteBackendInteractiveDetection:
    """Tests for RemoteBackend._detect_interactive_prompt()."""

    def test_detects_yn_prompt(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["Some output", "Do you want to continue? [y/N]"]
        result = backend._detect_interactive_prompt(lines)
        assert result is not None
        assert "confirmation" in result

    def test_detects_password_prompt(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["Connecting...", "Password:"]
        result = backend._detect_interactive_prompt(lines)
        assert result is not None
        assert "password" in result

    def test_detects_sudo_prompt(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["[sudo] password for user:"]
        result = backend._detect_interactive_prompt(lines)
        assert result is not None

    def test_no_prompt_returns_none(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["total 42", "-rw-r--r-- 1 user user 100 file.txt", "user@host:~$"]
        result = backend._detect_interactive_prompt(lines)
        assert result is None

    def test_detects_ssh_host_key(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["Are you sure you want to continue connecting (yes/no)?"]
        result = backend._detect_interactive_prompt(lines)
        assert result is not None

    def test_only_checks_last_5_lines(self, remote_backend):
        """Prompt detection only checks the last 5 lines."""
        backend, _, _ = remote_backend
        lines = [
            "Password: (old prompt from scrollback)",
            "normal line 1",
            "normal line 2",
            "normal line 3",
            "normal line 4",
            "normal line 5",
        ]
        # The Password: is in position 0, but only last 5 are checked
        result = backend._detect_interactive_prompt(lines)
        assert result is None


class TestRemoteBackendDetectBlockedTab:
    """Tests for RemoteBackend._detect_blocked_tab()."""

    def test_normal_prompt_not_blocked(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["some output", "user@host:~$"]
        result = backend._detect_blocked_tab("default", lines)
        assert result is None

    def test_hash_prompt_not_blocked(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["output", "root@host:#"]
        result = backend._detect_blocked_tab("default", lines)
        assert result is None

    def test_interactive_prompt_blocked(self, remote_backend):
        backend, _, _ = remote_backend
        lines = ["Install packages?", "[y/N]"]
        result = backend._detect_blocked_tab("default", lines)
        assert result is not None


class TestRemoteBackendShellOperations:
    """Tests for RemoteBackend shell tab management."""

    def _setup_exec_mock(self, mock_ssh, output: str = "", exit_code: int = 0):
        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(stdout_data=output.encode("utf-8"), exit_code=exit_code),
        )

    def test_init_existing_session_rehydrates_without_reset(self, remote_backend):
        backend, _, _ = remote_backend
        backend.connect()
        backend.set_shell_owner_token(1)

        with (
            patch.object(
                backend, "_create_or_observe_tmux_session", return_value="existing"
            ) as observe,
            patch.object(backend, "_promote_tmux_owner_token"),
            patch.object(backend, "_attest_tmux_owner"),
            patch.object(backend, "_ensure_prompt_token"),
            patch.object(
                backend,
                "_read_tmux_session_option",
                side_effect=lambda option: (
                    _TMUX_SETUP_COMPLETE if option == _TMUX_SETUP_OPTION else "3"
                ),
            ),
            patch.object(backend, "_rehydrate_tabs") as rehydrate,
            patch.object(backend, "_send_and_wait") as setup,
            patch.object(backend, "_tmux_mutate_checked") as mutate,
        ):
            backend._init_shell()

        observe.assert_called_once_with()
        rehydrate.assert_called_once_with()
        setup.assert_not_called()
        assert all(
            "kill-session" not in call_.args[0] for call_ in mutate.call_args_list
        )
        assert backend._shell_initialized is True

    def test_stateless_active_session_is_promoted_without_kill(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(1)

        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            assert backend._create_or_observe_tmux_session() == "created"

        command = execute.call_args.args[0]
        assert "tmux has-session" in command
        assert "tmux new-session" in command
        active_branch = command.split('if [ "$_srw_status" = active ]; then', 1)[
            1
        ].split('elif [ "$_srw_status" = creating ]; then', 1)[0]
        assert "printf existing" in active_branch
        assert "kill-session" not in active_branch

    def test_pinned_create_stays_destructive_and_never_adopts(self, remote_backend):
        backend, _, _ = remote_backend

        with patch.object(
            backend, "_tmux_exec_checked", return_value="created"
        ) as execute:
            assert backend._create_or_observe_tmux_session() == "created"

        command = execute.call_args.args[0]
        assert "tmux kill-session" in command
        assert "tmux new-session" in command
        assert "printf existing" not in command

    def test_setup_send_and_wait_is_guarded_and_cas_cleared(self, remote_backend):
        backend, _, _ = remote_backend
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch.object(backend, "_reserve_and_send_shell_command") as reserve,
            patch.object(
                backend,
                "_tmux_capture",
                return_value=["__READY_01234567__ 0 /workspace"],
            ),
            patch.object(backend, "_clear_tab_pending") as clear,
        ):
            uuid4.return_value.hex = "0123456789abcdef"
            backend._send_and_wait(
                "default", "export FOO=bar", expected_pending="old-guard"
            )

        reserve.assert_called_once_with(
            "default",
            expected="old-guard",
            sentinel=_INHERITED_BUSY_SENTINEL,
            command=(
                "export FOO=bar; _srw_setup_rc=$?; "
                "printf '\\n__READY_01234567__ %s %s\\n' "
                '"$_srw_setup_rc" "$PWD"'
            ),
        )
        clear.assert_called_once_with("default", _INHERITED_BUSY_SENTINEL)

    def test_fresh_session_persists_default_tab_type(self, remote_backend):
        backend, _, _ = remote_backend
        backend.connect()

        with (
            patch.object(
                backend, "_create_or_observe_tmux_session", return_value="created"
            ),
            patch.object(backend, "_promote_tmux_owner_token"),
            patch.object(backend, "_attest_tmux_owner"),
            patch.object(backend, "_ensure_prompt_token"),
            patch.object(
                backend,
                "_read_tmux_session_option",
                side_effect=lambda option: (
                    _TMUX_SETUP_PENDING if option == _TMUX_SETUP_OPTION else ""
                ),
            ),
            patch.object(backend, "_discover_single_pane", return_value="%1"),
            patch.object(backend, "_set_tmux_window_option") as set_option,
            patch.object(backend, "_send_and_wait"),
            patch.object(backend, "_install_prompt_marker"),
            patch.object(backend, "_tmux_mutate_checked"),
        ):
            backend._init_shell()

        set_option.assert_any_call("default", _TMUX_TAB_TYPE_OPTION, "shell")
        assert backend._tabs["default"].tab_type == "shell"
        assert backend._tabs["default"].pane_id == "%1"

    def test_reattach_restores_persisted_tab_type_and_pending_sentinel(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend._shell_protocol_current = True
        metadata = "\n".join(
            (
                _tmux_window_row(
                    "default", "shell", "__DONE_0123456789ab__", pane_id="%1"
                ),
                _tmux_window_row("console", "repl", pane_id="%2", stored_pane_id="%2"),
            )
        )

        with (
            patch.object(backend, "_tmux_exec_checked", return_value=metadata),
            patch.object(backend, "_tmux_capture", return_value=["still running"]),
        ):
            backend._rehydrate_tabs()

        assert list(backend._tabs) == ["default", "console"]
        assert backend._tabs["default"].tab_type == "shell"
        assert backend._tabs["default"].pending_sentinel == "__DONE_0123456789ab__"
        assert backend._tabs["default"].pane_id == "%1"
        assert backend._tabs["console"].tab_type == "repl"

    @pytest.mark.parametrize(
        ("metadata", "message"),
        (
            (_tmux_window_row("default", "shell", pane_count="2"), "topology"),
            (_tmux_window_row("Bad Name", "shell"), "represented safely"),
            ("default\x1fshell\x1fmissing-fields", "Malformed"),
        ),
    )
    def test_reattach_refuses_split_panes_and_malformed_metadata(
        self, remote_backend, metadata, message
    ):
        backend, _, _ = remote_backend
        with patch.object(backend, "_tmux_exec_checked", return_value=metadata):
            with pytest.raises(WorkspaceUnavailableError, match=message):
                backend._rehydrate_tabs()
        assert backend._tabs == {}

    def test_reattach_refuses_changed_pane_identity(self, remote_backend):
        backend, _, _ = remote_backend
        metadata = _tmux_window_row(
            "default", "shell", pane_id="%2", stored_pane_id="%1"
        )
        with patch.object(backend, "_tmux_exec_checked", return_value=metadata):
            with pytest.raises(WorkspaceUnavailableError, match="pane identity"):
                backend._rehydrate_tabs()

    def test_current_protocol_refuses_incomplete_default_window(self, remote_backend):
        backend, _, _ = remote_backend
        backend._shell_protocol_current = True
        metadata = _tmux_window_row("default", "shell", setup_state=_TMUX_SETUP_PENDING)

        with patch.object(backend, "_tmux_exec_checked", return_value=metadata):
            with pytest.raises(WorkspaceUnavailableError, match="setup is incomplete"):
                backend._rehydrate_tabs()

    def test_current_protocol_discards_incomplete_nondefault_window(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend._shell_protocol_current = True
        metadata = "\n".join(
            (
                _tmux_window_row("default", "shell"),
                _tmux_window_row(
                    "console",
                    "repl",
                    pane_id="%2",
                    stored_pane_id="%2",
                    setup_state=_TMUX_SETUP_PENDING,
                ),
            )
        )

        with (
            patch.object(backend, "_tmux_exec_checked", return_value=metadata),
            patch.object(backend, "_tmux_capture", return_value=["$", "#"]),
            patch.object(backend, "_tmux_mutate_checked") as mutate,
        ):
            backend._rehydrate_tabs()

        assert list(backend._tabs) == ["default"]
        assert any(
            "kill-window" in call_.args[0] and "console" in call_.args[0]
            for call_ in mutate.call_args_list
        )

    @pytest.mark.parametrize("tab_type", ("ssh", "repl"))
    def test_non_shell_window_is_complete_before_initial_command(
        self, remote_backend, tab_type
    ):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        lifecycle = MagicMock()
        with (
            patch.object(backend, "_tmux_mutate_checked") as mutate,
            patch.object(backend, "_discover_single_pane", return_value="%2"),
            patch.object(backend, "_set_tmux_window_option") as set_option,
            patch.object(backend, "_reserve_and_send_shell_command") as reserve,
        ):
            lifecycle.attach_mock(set_option, "option")
            lifecycle.attach_mock(reserve, "reserve")
            backend.shell_open_tab("console", command="python", tab_type=tab_type)

        assert _TMUX_WINDOW_SETUP_OPTION in mutate.call_args.args[0]
        complete = call.option(
            "console", _TMUX_WINDOW_SETUP_OPTION, _TMUX_SETUP_COMPLETE
        )
        assert complete in lifecycle.mock_calls
        assert lifecycle.mock_calls.index(complete) < next(
            i
            for i, recorded in enumerate(lifecycle.mock_calls)
            if recorded
            == call.reserve(
                "console",
                expected=None,
                sentinel=_INHERITED_BUSY_SENTINEL,
                command="python",
            )
        )

    def test_legacy_default_without_type_is_adopted_as_opaque_busy_shell(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        metadata = _tmux_window_row("default", "")

        with (
            patch.object(backend, "_tmux_exec_checked", return_value=metadata),
            patch.object(backend, "_tmux_mutate_checked"),
            patch.object(
                backend,
                "_tmux_capture",
                return_value=["agent@host:/workspace$"],
            ) as capture,
        ):
            backend._rehydrate_tabs()

        capture.assert_called_once_with("default")
        assert backend._tabs["default"].tab_type == "shell"
        assert backend._tabs["default"].pending_sentinel == _INHERITED_BUSY_SENTINEL

    @pytest.mark.parametrize(
        ("name", "stored_type"),
        (("default", "future-repl"), ("console", "")),
    )
    def test_unknown_or_nondefault_missing_type_is_not_assumed_to_be_shell(
        self, remote_backend, name, stored_type
    ):
        backend, _, _ = remote_backend
        metadata = _tmux_window_row(name, stored_type)

        with (
            patch.object(backend, "_tmux_exec_checked", return_value=metadata),
            patch.object(backend, "_tmux_capture") as capture,
        ):
            backend._rehydrate_tabs()

        capture.assert_not_called()
        assert backend._tabs[name].tab_type == "process"
        backend._shell_initialized = True
        with patch.object(backend, "_tmux_send_keys") as send:
            with pytest.raises(ValueError, match="process"):
                backend.shell_run("echo must-not-run", tab_name=name)
        send.assert_not_called()

    @pytest.mark.parametrize("stored_type", ("shell", ""))
    def test_inherited_unmarked_busy_shell_blocks_colliding_command(
        self, remote_backend, stored_type
    ):
        backend, _, _ = remote_backend
        metadata = _tmux_window_row("default", stored_type)
        terminal = ["compiling target ..."]

        with (
            patch.object(backend, "_tmux_exec_checked", return_value=metadata),
            patch.object(backend, "_tmux_mutate_checked") as mutate,
            patch.object(backend, "_tmux_capture", return_value=terminal),
        ):
            backend._rehydrate_tabs()

        tab = backend._tabs["default"]
        assert tab.pending_sentinel == _INHERITED_BUSY_SENTINEL
        assert any(
            _TMUX_PENDING_SENTINEL_OPTION in call_.args[0]
            for call_ in mutate.call_args_list
        )

        backend._shell_initialized = True
        with (
            patch.object(backend, "_tmux_capture", return_value=terminal),
            patch.object(backend, "_tmux_send_keys") as send,
        ):
            result = backend.shell_run("echo must-not-run")

        send.assert_not_called()
        assert "previous command still running" in result

    def test_shell_list_tabs_after_init(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()

        tabs = backend.shell_list_tabs()
        assert len(tabs) == 1
        assert tabs[0]["name"] == "default"

    def test_shell_format_tab_header(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()

        header = backend.shell_format_tab_header()
        assert "default" in header
        assert "[Shells:" in header

    def test_shell_open_tab(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            meta = backend.shell_open_tab("build")

        assert meta["name"] == "build"
        assert "build" in backend._tabs

    def test_shell_open_duplicate_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            with pytest.raises(ValueError, match="already exists"):
                backend.shell_open_tab("default")

    def test_shell_open_invalid_name_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            with pytest.raises(ValueError, match="Invalid tab name"):
                backend.shell_open_tab("UPPERCASE")

    def test_shell_open_exceeds_max_tabs_raises(self):
        mock_ssh = MagicMock()
        mock_sftp = MagicMock()
        transport = MagicMock()
        transport.is_active.return_value = True
        mock_ssh.get_transport.return_value = transport
        mock_ssh.open_sftp.return_value = mock_sftp

        _wire_exec_channel(mock_ssh, _WindowedChannel())

        with patch("paramiko.SSHClient", return_value=mock_ssh):
            backend = RemoteBackend(
                host="host", workspace_path="/ws", max_tabs=2, max_retries=1
            )
            backend.connect()
            with patch("time.sleep"):
                backend._init_shell()  # creates "default" tab (1/2)
                backend.shell_open_tab("second")  # (2/2)
                with pytest.raises(ValueError, match="Maximum tabs"):
                    backend.shell_open_tab("third")

    def test_shell_close_tab(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            backend.shell_open_tab("temp")
            result = backend.shell_close_tab("temp")

        assert "closed" in result
        assert "temp" not in backend._tabs

    def test_shell_close_nonexistent_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            with pytest.raises(KeyError, match="not found"):
                backend.shell_close_tab("ghost")

    def test_shell_ensure_tab_creates_if_missing(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            backend.shell_ensure_tab("auto")

        assert "auto" in backend._tabs

    def test_shell_ensure_tab_noop_if_exists(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            # default already exists; should not raise or create duplicate
            backend.shell_ensure_tab("default")

        assert list(backend._tabs.keys()).count("default") == 1

    def test_concurrent_shell_ensure_tab_creates_named_window_once(
        self, remote_backend
    ):
        """A same-response shell batch must not race a new named tmux tab."""
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")
        release_create = threading.Event()
        first_create = threading.Event()
        create_count = 0
        count_lock = threading.Lock()
        errors = []

        def open_tab(name, command=None, tab_type=None):
            nonlocal create_count
            with count_lock:
                create_count += 1
                first_create.set()
            assert release_create.wait(timeout=2)
            backend._tabs[name] = _RemoteTab(name, pane_id="%2")
            return backend._tabs[name].to_metadata()

        def ensure():
            try:
                backend.shell_ensure_tab("verify")
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with patch.object(backend, "shell_open_tab", side_effect=open_tab):
            first = threading.Thread(target=ensure)
            first.start()
            assert first_create.wait(timeout=1)

            followers = [threading.Thread(target=ensure) for _ in range(3)]
            for thread in followers:
                thread.start()

            # Keep the first create in flight until every follower has had a
            # chance to hit the absent-tab path.  With no admission lock all
            # four enter open_tab; with the fix only the first can enter.
            time.sleep(0.1)
            release_create.set()
            first.join(timeout=2)
            for thread in followers:
                thread.join(timeout=2)

        assert not first.is_alive()
        assert all(not thread.is_alive() for thread in followers)
        assert errors == []
        assert create_count == 1
        assert backend._tabs["verify"].pane_id == "%2"

    def test_shell_is_alive(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Setup exec for init (empty — _init_shell discards its own output)
        _wire_exec_channel(mock_ssh, _WindowedChannel())

        with patch("time.sleep"):
            backend._init_shell()

        # Re-wire with the is_alive check's actual output: the channel above
        # is a single-shot drain, already consumed by _init_shell's _exec calls.
        _wire_exec_channel(mock_ssh, _WindowedChannel(stdout_data=b"yes\n"))

        assert backend.shell_is_alive() is True

    def test_shell_is_alive_not_initialized(self, remote_backend):
        backend, _, _ = remote_backend
        assert backend.shell_is_alive() is False

    def test_shell_cleanup(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        _wire_exec_channel(mock_ssh, _WindowedChannel())

        with patch("time.sleep"):
            backend._init_shell()

        backend.shell_cleanup()
        assert backend._shell_initialized is False
        assert len(backend._tabs) == 0

    def test_shell_cleanup_explicitly_destroys_remote_session(self, remote_backend):
        backend, _, _ = remote_backend
        backend.connect()
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()

        execute.assert_called_once()
        command = execute.call_args.args[0]
        assert "flock -o -w 30" in command
        assert "kill-session" in command
        assert _TMUX_OWNER_ID_OPTION in command
        assert execute.call_args.kwargs == {
            "operation": "kill session",
            "allow_shell_retired": True,
        }
        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_shell_cleanup_without_local_init_still_destroys_inherited_session(
        self, remote_backend
    ):
        backend, _, _ = remote_backend

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_cleanup()

        execute.assert_called_once()
        assert "tmux has-session" in execute.call_args.args[0]
        assert "kill-session" in execute.call_args.args[0]

    def test_strict_pinned_shell_cleanup_propagates_lost_ack(self, remote_backend):
        backend, _, _ = remote_backend

        with patch.object(
            backend,
            "_tmux_exec_checked",
            side_effect=RuntimeError("SSH response lost"),
        ):
            with pytest.raises(
                WorkspaceUnavailableError,
                match="terminal retirement was not acknowledged",
            ):
                backend.shell_cleanup_strict()

        assert backend._strict_terminal_cleanup is False

    def test_timeout_reset_requires_exact_pinned_owner_before_kill(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

        with patch.object(backend, "_tmux_exec_checked") as execute:
            backend.shell_reset_after_timeout()

        execute.assert_called_once()
        command = execute.call_args.args[0]
        owner_check = f'"$_srw_id" = {backend._job_id}'
        assert owner_check in command
        assert command.index(owner_check) < command.index("tmux kill-session")
        assert "tmux has-session" in command
        assert "|| exit 0" in command
        assert execute.call_args.kwargs == {
            "operation": "reset timed-out pinned shell session",
        }
        assert backend._shell_initialized is False
        assert backend._tabs == {}

    def test_timeout_reset_uses_stateless_generation_fence(self, remote_backend):
        backend, _, _ = remote_backend
        backend.set_shell_owner_token(17)

        with patch.object(backend, "_reset_stateless_tmux_session") as reset:
            backend.shell_reset_after_timeout()

        reset.assert_called_once_with()


class TestRemoteBackendShellRun:
    """Sentinel output reports CWD and working_dir calls restore the tab."""

    _ROOT = "/home/agent-host/workspace"
    _SENTINEL = "__DONE_0123456789ab__"

    @staticmethod
    def _ready(backend):
        backend._shell_initialized = True
        backend._tabs["default"] = _RemoteTab("default", pane_id="%1")

    @staticmethod
    def _uuid(uuid4):
        uuid4.return_value.hex = "0123456789abcdef"

    def test_pending_sentinel_is_persisted_before_command_and_cleared_afterward(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        self._ready(backend)
        captures = [
            [f"agent@host:{self._ROOT}$"],
            [
                f"agent@host:{self._ROOT}$",
                f"{self._SENTINEL} 0 {self._ROOT}",
            ],
        ]

        def reserve_pending(_tab_name, *, sentinel, **_kwargs):
            backend._tabs["default"].pending_sentinel = sentinel

        def clear_pending(_tab_name, _expected):
            backend._tabs["default"].pending_sentinel = None

        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(backend, "_tmux_capture", side_effect=captures),
            patch.object(
                backend,
                "_reserve_and_send_shell_command",
                side_effect=reserve_pending,
            ) as reserve,
            patch.object(
                backend, "_clear_tab_pending", side_effect=clear_pending
            ) as clear,
        ):
            lifecycle = MagicMock()
            lifecycle.attach_mock(reserve, "reserve")
            lifecycle.attach_mock(clear, "clear")
            self._uuid(uuid4)
            result = backend.shell_run("pwd")

        reserve.assert_called_once_with(
            "default",
            expected=None,
            sentinel=self._SENTINEL,
            command=reserve.call_args.kwargs["command"],
        )
        assert "pwd" in reserve.call_args.kwargs["command"]
        clear.assert_called_once_with("default", self._SENTINEL)
        assert lifecycle.mock_calls[0] == call.reserve(
            "default",
            expected=None,
            sentinel=self._SENTINEL,
            command=reserve.call_args.kwargs["command"],
        )
        assert lifecycle.mock_calls[-1] == call.clear("default", self._SENTINEL)
        assert "Exit code: 0" in result

    def test_commands_on_distinct_tabs_do_not_serialize(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)
        backend._tabs["git"] = _RemoteTab("git", pane_id="%2")
        polling_child = threading.Event()
        release_child = threading.Event()
        git_finished = threading.Event()
        sentinels = {}
        capture_counts = {"default": 0, "git": 0}
        results = {}

        def reserve(tab_name, *, sentinel, **_kwargs):
            sentinels[tab_name] = sentinel
            backend._tabs[tab_name].pending_sentinel = sentinel

        def capture(tab_name):
            capture_counts[tab_name] += 1
            if capture_counts[tab_name] == 1:
                return []
            if tab_name == "default":
                polling_child.set()
                release_child.wait(timeout=2)
            return [f"{sentinels[tab_name]} 0 {self._ROOT}"]

        def clear(tab_name, _expected):
            backend._tabs[tab_name].pending_sentinel = None

        def run_child():
            results["child"] = backend.shell_run("sleep 180", tab_name="default")

        def run_git():
            results["git"] = backend.shell_run("git status", tab_name="git")
            git_finished.set()

        with (
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(backend, "_tmux_capture", side_effect=capture),
            patch.object(
                backend, "_reserve_and_send_shell_command", side_effect=reserve
            ),
            patch.object(backend, "_clear_tab_pending_if_current", side_effect=clear),
        ):
            child_thread = threading.Thread(target=run_child)
            git_thread = threading.Thread(target=run_git)
            child_thread.start()
            assert polling_child.wait(timeout=1)
            git_thread.start()
            ran_concurrently = git_finished.wait(timeout=1)
            release_child.set()
            child_thread.join(timeout=2)
            git_thread.join(timeout=2)

        assert ran_concurrently
        assert not child_thread.is_alive()
        assert not git_thread.is_alive()
        assert "Exit code: 0" in results["child"]
        assert "Exit code: 0" in results["git"]

    def test_commands_on_same_tab_share_serialization_lock(self, remote_backend):
        backend, _, _ = remote_backend

        assert backend._shell_tab_lock("default") is backend._shell_tab_lock("default")
        assert backend._shell_tab_lock("default") is not backend._shell_tab_lock("git")

    def test_long_cwd_completion_survives_tmux_display_wrapping(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)
        long_cwd = "/workspace/" + "nested-directory/" * 30
        captures = 0

        def capture(command, *, operation, pane_context):
            nonlocal captures
            assert "capture-pane -J " in command
            assert operation == "capture pane default"
            assert pane_context == ("default", "%1")
            captures += 1
            if captures == 1:
                return "$\n"
            return f"{self._SENTINEL} 0 {long_cwd}\n"

        def reserve(_tab_name, *, sentinel, **_kwargs):
            backend._tabs["default"].pending_sentinel = sentinel

        def clear(_tab_name, _expected):
            backend._tabs["default"].pending_sentinel = None

        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(backend, "_tmux_exec_checked", side_effect=capture),
            patch.object(
                backend, "_reserve_and_send_shell_command", side_effect=reserve
            ) as send,
            patch.object(backend, "_clear_tab_pending", side_effect=clear),
        ):
            self._uuid(uuid4)
            result = backend.shell_run("pwd")

        send.assert_called_once()
        assert f"CWD: {long_cwd}" in result
        assert backend._tabs["default"].pending_sentinel is None

    def test_idle_guard_ignores_stale_prompt_like_scrollback(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)

        def reserve(_tab_name, *, sentinel, **_kwargs):
            backend._tabs["default"].pending_sentinel = sentinel

        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(
                backend,
                "_tmux_capture",
                side_effect=[
                    ["old command", "Continue? [y/N]"],
                    [f"{self._SENTINEL} 0 {self._ROOT}"],
                ],
            ),
            patch.object(
                backend, "_reserve_and_send_shell_command", side_effect=reserve
            ) as send,
            patch.object(backend, "_clear_tab_pending_if_current") as clear,
        ):
            self._uuid(uuid4)
            result = backend.shell_run("echo admitted")

        send.assert_called_once()
        clear.assert_called_once_with("default", self._SENTINEL)
        assert "Exit code: 0" in result

    def test_pending_guard_still_reports_genuine_blocking_prompt(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)
        backend._tabs["default"].pending_sentinel = "__DONE_aaaaaaaaaaaa__"

        with (
            patch.object(backend, "_tmux_capture", return_value=["Continue? [y/N]"]),
            patch.object(backend, "_reserve_and_send_shell_command") as send,
        ):
            result = backend.shell_run("echo must-not-run")

        send.assert_not_called()
        assert "blocked by a previous confirmation prompt" in result

    def test_working_dir_restores_between_calls(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)
        captures = [
            [f"agent@host:{self._ROOT}$"],
            [f"agent@host:{self._ROOT}$", f"{self._SENTINEL} 0 /tmp"],
            [f"agent@host:{self._ROOT}$"],
            [f"agent@host:{self._ROOT}$", f"{self._SENTINEL} 0 {self._ROOT}"],
        ]

        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(backend, "_tmux_capture", side_effect=captures),
            patch.object(backend, "_reserve_and_send_shell_command") as reserve,
            patch.object(backend, "_tmux_send_keys") as send,
            patch.object(backend, "_clear_tab_pending"),
        ):
            self._uuid(uuid4)
            first = backend.shell_run(
                "cd /tmp && pwd", working_dir=".", tab_name="default"
            )
            second = backend.shell_run("pwd", working_dir=".", tab_name="default")

        assert "CWD: /tmp" in first
        assert f"CWD: {self._ROOT}" in second
        send.assert_not_called()
        assert '"$PWD"' in reserve.call_args_list[0].kwargs["command"]
        assert f"cd {self._ROOT}" in reserve.call_args_list[0].kwargs["command"]
        assert len(reserve.call_args_list) == 2

    def test_without_working_dir_keeps_persistent_cwd_visible(self, remote_backend):
        backend, _, _ = remote_backend
        self._ready(backend)
        captures = [
            [f"agent@host:{self._ROOT}$"],
            [f"agent@host:{self._ROOT}$", f"{self._SENTINEL} 0 /tmp"],
            ["agent@host:/tmp$"],
            ["agent@host:/tmp$", f"{self._SENTINEL} 0 /tmp"],
        ]

        with (
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch.object(backend, "_tmux_capture", side_effect=captures),
            patch.object(backend, "_reserve_and_send_shell_command") as reserve,
            patch.object(backend, "_tmux_send_keys") as send,
            patch.object(backend, "_clear_tab_pending"),
        ):
            self._uuid(uuid4)
            first = backend.shell_run("cd /tmp && pwd", tab_name="default")
            second = backend.shell_run("pwd", tab_name="default")

        assert "CWD: /tmp" in first
        assert "CWD: /tmp" in second
        assert all(sent.args[1] != f"cd {self._ROOT}" for sent in send.call_args_list)
        assert len(reserve.call_args_list) == 2


class TestRemoteBackendShellSend:
    """Tests for RemoteBackend.shell_send()."""

    def _setup_exec_mock(self, mock_ssh, output: str = "", exit_code: int = 0):
        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(stdout_data=output.encode("utf-8"), exit_code=exit_code),
        )

    def test_send_to_existing_tab(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            result = backend.shell_send("default", "ls -la")

        assert "Sent" in result

    def test_send_to_nonexistent_tab_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            with pytest.raises(KeyError, match="not found"):
                backend.shell_send("ghost", "ls")

    def test_async_command_refuses_to_collide_with_inherited_busy_pane(
        self, remote_backend
    ):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        tab = _RemoteTab("default", pane_id="%1")
        tab.pending_sentinel = _INHERITED_BUSY_SENTINEL
        backend._tabs["default"] = tab

        with (
            patch.object(backend, "_tmux_capture", return_value=["still running"]),
            patch.object(backend, "_tmux_send_keys") as send,
            patch.object(backend, "_reserve_and_send_shell_command") as reserve,
        ):
            result = backend.shell_send("default", "second command")

        assert "previous command still running" in result
        send.assert_not_called()
        reserve.assert_not_called()

    def test_explicit_busy_input_can_feed_foreground_process(self, remote_backend):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        tab = _RemoteTab("default", pane_id="%1")
        tab.pending_sentinel = _INHERITED_BUSY_SENTINEL
        backend._tabs["default"] = tab

        with (
            patch.object(backend, "_tmux_capture") as capture,
            patch.object(backend, "_tmux_send_keys") as send,
        ):
            result = backend.shell_send("default", "yes", enter=True, allow_busy=True)

        assert result == "Sent to 'default'"
        capture.assert_not_called()
        send.assert_called_once_with("default", "yes", enter=True)
        assert tab.pending_sentinel == _INHERITED_BUSY_SENTINEL

    def test_send_blocked_command_with_enter(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            result = backend.shell_send("default", "reboot", enter=True)

        assert "blocked" in result.lower()

    def test_send_blocked_command_without_enter_allowed(self, remote_backend):
        """When enter=False, blocked command check is skipped (just keystrokes)."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
            result = backend.shell_send("default", "reboot", enter=False)

        assert "Sent" in result

    def test_async_working_dir_wraps_command_and_restore(self, remote_backend):
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
        with patch.object(backend, "_reserve_and_send_shell_command") as reserve:
            backend.shell_send(
                "default",
                "npm run dev",
                working_dir="repo",
            )

        sent = reserve.call_args.kwargs["command"]
        assert sent.startswith(f"cd {backend._sandbox_cwd}/repo && ")
        assert '_srw_cwd="$PWD"' in sent
        assert "npm run dev" in sent
        assert f"cd {backend._sandbox_cwd}" in sent
        assert re.search(r"__DONE_[0-9a-f]{12}__", sent)

    def test_working_dir_rejected_for_raw_keystrokes(self, remote_backend):
        backend, mock_ssh, _ = remote_backend
        backend.connect()
        self._setup_exec_mock(mock_ssh)

        with patch("time.sleep"):
            backend._init_shell()
        with pytest.raises(ValueError, match="raw keystrokes"):
            backend.shell_send(
                "default",
                "C-c",
                enter=False,
                working_dir="repo",
            )


class TestRemoteBackendShellCancel:
    """Cancellation is released only by its exact completion probe."""

    @staticmethod
    def _ready(remote_backend, pending=None):
        backend, _, _ = remote_backend
        backend._shell_initialized = True
        tab = _RemoteTab("default", pane_id="%1")
        tab.pending_sentinel = pending
        backend._tabs["default"] = tab
        return backend, tab

    def test_noop_when_tab_idle(self, remote_backend):
        backend, _ = self._ready(remote_backend)
        with patch.object(backend, "_cancel_and_probe_shell_command") as cancel:
            result = backend.shell_cancel("default")
        cancel.assert_not_called()
        assert "nothing" in result.lower()

    def test_exact_probe_record_frees_tab(self, remote_backend):
        backend, tab = self._ready(remote_backend, "__DONE_aaaaaaaaaaaa__")

        def install_probe(_name, *, expected, sentinel):
            assert expected == "__DONE_aaaaaaaaaaaa__"
            tab.pending_sentinel = sentinel

        def clear_probe(_name, expected):
            assert expected == "__DONE_bbbbbbbbbbbb__"
            tab.pending_sentinel = None

        with (
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch.object(
                backend,
                "_cancel_and_probe_shell_command",
                side_effect=install_probe,
            ) as cancel,
            patch.object(
                backend,
                "_tmux_capture",
                return_value=["__DONE_bbbbbbbbbbbb__ 130 /workspace"],
            ),
            patch.object(
                backend, "_clear_tab_pending_if_current", side_effect=clear_probe
            ) as clear,
        ):
            uuid4.return_value.hex = "b" * 32
            result = backend.shell_cancel("default")
        cancel.assert_called_once_with(
            "default",
            expected="__DONE_aaaaaaaaaaaa__",
            sentinel="__DONE_bbbbbbbbbbbb__",
        )
        clear.assert_called_once_with("default", "__DONE_bbbbbbbbbbbb__")
        assert tab.pending_sentinel is None
        assert "free" in result.lower()

    def test_long_cwd_probe_survives_tmux_display_wrapping(self, remote_backend):
        backend, tab = self._ready(remote_backend, "__DONE_aaaaaaaaaaaa__")
        long_cwd = "/workspace/" + "nested-directory/" * 30

        def install_probe(_name, *, expected, sentinel):
            assert expected == "__DONE_aaaaaaaaaaaa__"
            tab.pending_sentinel = sentinel

        def capture(command, *, operation, pane_context):
            assert "capture-pane -J " in command
            assert operation == "capture pane default"
            assert pane_context == ("default", "%1")
            return f"__DONE_bbbbbbbbbbbb__ 130 {long_cwd}\n"

        def clear(_name, _expected):
            tab.pending_sentinel = None

        with (
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch.object(
                backend,
                "_cancel_and_probe_shell_command",
                side_effect=install_probe,
            ),
            patch.object(backend, "_tmux_exec_checked", side_effect=capture),
            patch.object(backend, "_clear_tab_pending_if_current", side_effect=clear),
        ):
            uuid4.return_value.hex = "b" * 32
            result = backend.shell_cancel("default")

        assert tab.pending_sentinel is None
        assert "free" in result.lower()

    def test_prompt_looking_text_never_frees_cancel_guard(self, remote_backend):
        backend, tab = self._ready(remote_backend, "__DONE_aaaaaaaaaaaa__")

        def install_probe(_name, *, expected, sentinel):
            assert expected == tab.pending_sentinel
            tab.pending_sentinel = sentinel

        with (
            patch("shared.runtime.core.backends.remote.time.sleep"),
            patch("shared.runtime.core.backends.remote.uuid.uuid4") as uuid4,
            patch.object(
                backend,
                "_cancel_and_probe_shell_command",
                side_effect=install_probe,
            ) as cancel,
            patch.object(
                backend,
                "_tmux_capture",
                side_effect=[["$"], ["#"]],
            ),
            patch.object(backend, "shell_close_tab") as close,
            patch.object(backend, "shell_ensure_tab") as ensure,
        ):
            uuid4.side_effect = [
                MagicMock(hex="b" * 32),
                MagicMock(hex="c" * 32),
            ]
            result = backend.shell_cancel("default")
        assert cancel.call_count == 2
        close.assert_called_once_with("default")
        ensure.assert_called_once_with("default")
        assert "reset" in result.lower()

    def test_nonexistent_tab_raises(self, remote_backend):
        backend, _ = self._ready(remote_backend)
        with pytest.raises(KeyError, match="not found"):
            backend.shell_cancel("ghost")


class TestRemoteBackendShellRead:
    """Tests for RemoteBackend.shell_read() and shell_read_with_offset()."""

    def _setup_exec_mock(self, mock_ssh, output: str = "", exit_code: int = 0):
        _wire_exec_channel(
            mock_ssh,
            _WindowedChannel(stdout_data=output.encode("utf-8"), exit_code=exit_code),
        )

    def test_shell_read_tail_mode(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        # Init shell with empty output
        self._setup_exec_mock(mock_ssh, "")
        with patch("time.sleep"):
            backend._init_shell()

        # Now set up capture output
        lines = "\n".join([f"line {i}" for i in range(20)])
        self._setup_exec_mock(mock_ssh, lines)

        text, meta = backend.shell_read("default", lines=5)
        assert meta["tab"] == "default"
        assert meta["mode"] == "tail"

    def test_shell_read_since_cursor(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        self._setup_exec_mock(mock_ssh, "")
        with patch("time.sleep"):
            backend._init_shell()

        lines = "\n".join([f"line {i}" for i in range(10)])
        self._setup_exec_mock(mock_ssh, lines)

        text, meta = backend.shell_read("default", since_cursor=True)
        assert meta["mode"] == "since_cursor"

    def test_shell_read_nonexistent_tab_raises(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        self._setup_exec_mock(mock_ssh, "")
        with patch("time.sleep"):
            backend._init_shell()
            with pytest.raises(KeyError, match="not found"):
                backend.shell_read("ghost")

    def test_shell_read_with_offset(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        self._setup_exec_mock(mock_ssh, "")
        with patch("time.sleep"):
            backend._init_shell()

        lines = "\n".join([f"line {i}" for i in range(50)])
        self._setup_exec_mock(mock_ssh, lines)

        text, meta = backend.shell_read_with_offset("default", lines=10, offset=5)
        assert meta["mode"] == "offset"
        assert meta["offset"] == 5

    def test_shell_read_with_offset_tail(self, remote_backend):
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()

        self._setup_exec_mock(mock_ssh, "")
        with patch("time.sleep"):
            backend._init_shell()

        lines = "\n".join([f"line {i}" for i in range(50)])
        self._setup_exec_mock(mock_ssh, lines)

        text, meta = backend.shell_read_with_offset("default", lines=10, offset=None)
        assert meta["mode"] == "tail"


class TestRemoteBackendRemoteTab:
    """Tests for the _RemoteTab helper class."""

    def test_tab_metadata(self):
        tab = _RemoteTab(name="test-tab", tab_type="shell")
        meta = tab.to_metadata()
        assert meta["name"] == "test-tab"
        assert meta["type"] == "shell"
        assert "created_at" in meta
        assert "last_activity" in meta

    def test_tab_defaults(self):
        tab = _RemoteTab(name="default")
        assert tab.tab_type == "shell"
        assert tab.read_cursor == 0


# =============================================================================
# Backend interface compliance
# =============================================================================


class TestBackendInterfaceCompliance:
    """Verify RemoteBackend implements the full abstract interface."""

    def test_remote_backend_has_all_abstract_methods(self):
        backend = RemoteBackend(host="host", workspace_path="/ws")
        abstract_methods = [
            "read_file",
            "write_file",
            "append_file",
            "exists",
            "is_file",
            "is_dir",
            "list_dir",
            "search_files",
            "mkdir",
            "delete_file",
            "delete_directory",
            "move",
            "copy",
            "stat",
            "resolve_path",
            "connect",
            "disconnect",
            "is_connected",
            "root",
        ]
        for method in abstract_methods:
            assert hasattr(backend, method), f"RemoteBackend missing {method}"

    def test_remote_backend_has_shell_methods(self):
        backend = RemoteBackend(host="host", workspace_path="/ws")
        shell_methods = [
            "shell_run",
            "shell_send",
            "shell_read",
            "shell_read_with_offset",
            "shell_ensure_tab",
            "shell_open_tab",
            "shell_close_tab",
            "shell_list_tabs",
            "shell_format_tab_header",
            "shell_cleanup",
            "shell_is_alive",
        ]
        for method in shell_methods:
            assert hasattr(backend, method), f"RemoteBackend missing {method}"


class TestConnectionHardening:
    """Tests for transport keepalive + SFTP channel timeout hardening."""

    def test_connect_sets_keepalive_and_sftp_timeout(self, remote_backend):
        """A blackholed connection must eventually error, not wait forever:
        transport keepalive + a socket timeout on the shared SFTP channel
        (which serializes ALL file ops behind _sftp_lock)."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_ssh.get_transport.return_value.set_keepalive.assert_called_with(15)
        mock_sftp.get_channel.return_value.settimeout.assert_called_with(60.0)

    def test_connect_sets_transport_socket_options(self, remote_backend):
        """Connect applies kernel socket options for long-halt recovery."""
        backend, mock_ssh, _ = remote_backend
        backend.connect()

        transport = mock_ssh.get_transport.return_value
        transport.set_keepalive.assert_called_with(_TRANSPORT_KEEPALIVE_SECONDS)
        sock = transport.get_socket.return_value

        sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt.assert_any_call(
                socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _TCP_KEEPALIVE_IDLE_SECONDS
            )
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt.assert_any_call(
                socket.IPPROTO_TCP,
                socket.TCP_KEEPINTVL,
                _TCP_KEEPALIVE_INTERVAL_SECONDS,
            )
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt.assert_any_call(
                socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _TCP_KEEPALIVE_COUNT
            )
        if hasattr(socket, "TCP_USER_TIMEOUT"):
            sock.setsockopt.assert_any_call(
                socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, _TCP_USER_TIMEOUT_MILLIS
            )

    def test_read_file_timeout_is_not_file_not_found(self, remote_backend):
        """socket.timeout is an OSError; without special-casing it,
        read_file reports a hung workspace as 'file not found'."""
        backend, mock_ssh, mock_sftp = remote_backend
        backend.connect()
        mock_sftp.open.side_effect = socket.timeout("timed out")
        with pytest.raises(RemoteCommandTimeoutError, match="timed out reading"):
            backend.read_file("some/file.md")
