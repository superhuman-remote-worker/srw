"""A full workspace volume is named as such, and the shell can still start once.

knowledge-base/knowledge/issues/workspace_disk_full_pauses_job_as_transport_lost.md:
the stateless shell rewrites its PVC-resident ownership record on every claim.
On a full volume that write failed with exit 78, the code for a malformed or
foreign record, and the job paused as "workspace transport lost". The write
now spends a small reserve once (the shell starts and the agent is told) and
otherwise exits 82 with the volume's usage. 78 keeps fencing.

The generated scripts run here for real under bash, with fake ``tmux``,
``mktemp`` and ``df``: the fake ``mktemp`` hands out symlinks to /dev/full, so
the record write gets a genuine ENOSPC.
"""

import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.workspace_backend import (
    WorkspaceDiskFullError,
    WorkspaceUnavailableError,
    completion_error_payload,
)
from shared.workspace_disk import (
    WORKSPACE_DISK_FULL_REASON,
    WorkspaceDiskFullReport,
    WorkspaceDiskUsage,
    WorkspaceVolumeGrowth,
    format_bytes,
    workspace_disk_full_action,
)

_WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_RUNTIME_INCARNATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
_FULL_DF = ("10255636", "9750000", "0", "655360", "12345", "643015")
_ROOMY_DF = ("10255636", "1000000", "9255636", "655360", "12345", "643015")


@pytest.fixture(autouse=True)
def _valid_private_key_for_mocked_ssh():
    with patch(
        "shared.runtime.core.backends.remote._validate_private_key",
        return_value="SHA256:test-fingerprint",
    ):
        yield


def _backend(token: int = 21) -> RemoteBackend:
    backend = RemoteBackend(
        host="workspace.test",
        workspace_path="/home/agent-host/workspace",
        job_id="aaaa-bbbb-cccc-dddd",
        workspace_generation=_WORKSPACE_GENERATION,
        runtime_incarnation=_RUNTIME_INCARNATION,
    )
    backend.set_shell_owner_token(token)
    return backend


class _Workspace:
    """A temporary $HOME whose volume can be made to refuse record writes."""

    def __init__(self, tmp_path: Path, *, full_writes: int, df: tuple[str, ...]):
        self.home = tmp_path / "home"
        self.home.mkdir()
        self.state_dir = self.home / ".srw" / "tmux"
        self.reserve = self.home / ".srw" / "disk.reserve"
        self.mktemp_calls = tmp_path / "mktemp-calls"
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        real_mktemp = shutil.which("mktemp")
        self._script(
            bin_dir / "tmux",
            'if [ "$1" = "has-session" ]; then exit 1; fi\nexit 0\n',
        )
        self._script(
            bin_dir / "mktemp",
            f'n=$(cat "{self.mktemp_calls}" 2>/dev/null || echo 0)\n'
            f'n=$((n + 1)); echo "$n" > "{self.mktemp_calls}"\n'
            f'if [ "$n" -le {full_writes} ]; then\n'
            f'  path=$("{real_mktemp}" -u "$1")\n'
            '  ln -s /dev/full "$path" && printf "%s\\n" "$path"\n'
            "  exit $?\n"
            "fi\n"
            f'exec "{real_mktemp}" "$@"\n',
        )
        size, used, available, inodes, inodes_used, inodes_free = df
        self._script(
            bin_dir / "df",
            'case "$1" in\n'
            "  -Pi) echo 'Filesystem Inodes IUsed IFree IUse% Mounted on'\n"
            f"       echo '/dev/longhorn/pvc {inodes} {inodes_used} {inodes_free} "
            "2% /home/agent-host' ;;\n"
            "  *) echo 'Filesystem 1024-blocks Used Available Capacity Mounted on'\n"
            f"     echo '/dev/longhorn/pvc {size} {used} {available} "
            "100% /home/agent-host' ;;\n"
            "esac\n",
        )
        self.env = dict(os.environ)
        self.env["HOME"] = str(self.home)
        self.env["PATH"] = f"{bin_dir}:{self.env['PATH']}"

    @staticmethod
    def _script(path: Path, body: str) -> None:
        path.write_text("#!/bin/sh\n" + body)
        path.chmod(0o755)

    def run(self, command: str, **_: object) -> tuple[str, int]:
        """Stand-in for ``RemoteBackend._exec_with_status`` on this workspace."""
        completed = subprocess.run(
            ["bash", "-c", command],
            env=self.env,
            text=True,
            capture_output=True,
            check=False,
            timeout=60,
        )
        return completed.stdout, completed.returncode

    def leftovers(self) -> list[str]:
        return sorted(p.name for p in self.state_dir.glob(".state.*"))


def _claim(backend: RemoteBackend, workspace: _Workspace) -> str:
    with patch.object(backend, "_exec_with_status", side_effect=workspace.run):
        return backend._stateless_create_or_observe_tmux_session()


class TestStateWriteOnFullVolume:
    def test_full_volume_without_reserve_exits_82_with_usage(self, tmp_path):
        workspace = _Workspace(tmp_path, full_writes=99, df=_FULL_DF)
        backend = _backend()

        with pytest.raises(WorkspaceDiskFullError) as raised:
            _claim(backend, workspace)

        message = str(raised.value)
        assert "failed with exit code 82" in message
        assert "workspace disk is full (9.3 GiB of 9.8 GiB used, 0 B free)" in message
        report = raised.value.report
        assert report.usage == WorkspaceDiskUsage.from_df(_FULL_DF)
        # The shell was starting: no command can have been running.
        assert report.command_in_flight is False
        assert not (workspace.state_dir / backend._tmux_state_filename).exists()
        assert workspace.leftovers() == []
        assert backend._shell_initialized is False

    def test_reserve_lets_the_record_write_once_and_tells_the_agent(self, tmp_path):
        workspace = _Workspace(tmp_path, full_writes=1, df=_FULL_DF)
        workspace.reserve.parent.mkdir(parents=True)
        workspace.reserve.write_bytes(b"\0" * 4096)
        backend = _backend()

        assert _claim(backend, workspace) == "created"

        assert not workspace.reserve.exists()  # spent, and not recreated
        fields = (
            (workspace.state_dir / backend._tmux_state_filename)
            .read_text()
            .strip()
            .split("|")
        )
        assert fields[:6] == [
            "3",
            backend._tmux_owner_digest,
            _WORKSPACE_GENERATION,
            _RUNTIME_INCARNATION,
            "active",
            "21",
        ]
        notice = backend.take_disk_full_notice()
        assert notice is not None
        assert notice.startswith("⚠ Workspace disk is full (9.3 GiB of 9.8 GiB used")
        assert "pip cache purge" in notice
        assert backend.take_disk_full_notice() is None  # delivered once
        assert workspace.leftovers() == []

    def test_spent_reserve_comes_back_once_the_volume_has_room(self, tmp_path):
        workspace = _Workspace(tmp_path, full_writes=0, df=_ROOMY_DF)
        backend = _backend()

        assert _claim(backend, workspace) == "created"

        assert workspace.reserve.stat().st_size == 8 * 1024 * 1024
        assert backend.take_disk_full_notice() is None

    def test_no_reserve_is_made_while_the_volume_is_nearly_full(self, tmp_path):
        nearly_full = ("10255636", "10200000", "55636", "655360", "12345", "643015")
        workspace = _Workspace(tmp_path, full_writes=0, df=nearly_full)

        assert _claim(_backend(), workspace) == "created"

        assert not workspace.reserve.exists()

    @pytest.mark.parametrize(
        "record",
        [
            "3|someone-else|x|y|active|21|" + "a" * 32 + "\n",
            "garbage\nsecond line\n",
            "",
        ],
        ids=["foreign-owner", "two-lines", "empty"],
    )
    def test_corrupt_or_foreign_record_still_fences_with_78(self, tmp_path, record):
        workspace = _Workspace(tmp_path, full_writes=99, df=_FULL_DF)
        workspace.state_dir.mkdir(parents=True)
        backend = _backend()
        state = workspace.state_dir / backend._tmux_state_filename
        state.write_text(record)

        with pytest.raises(WorkspaceUnavailableError) as raised:
            _claim(backend, workspace)

        assert not isinstance(raised.value, WorkspaceDiskFullError)
        assert "exit code 78" in str(raised.value)
        assert state.read_text() == record
        # The load refused before any write was attempted.
        assert not workspace.mktemp_calls.exists()

    def test_failed_write_on_a_roomy_volume_is_not_called_disk_full(self, tmp_path):
        workspace = _Workspace(tmp_path, full_writes=99, df=_ROOMY_DF)

        with pytest.raises(WorkspaceUnavailableError) as raised:
            _claim(_backend(), workspace)

        assert not isinstance(raised.value, WorkspaceDiskFullError)
        assert str(raised.value).endswith(
            "exit code 82: could not write the shell ownership record"
        )


class TestDiskFullClassification:
    def test_exit_82_from_a_running_shell_keeps_command_in_flight(self):
        backend = _backend()
        output = "srw-state-write-failed " + " ".join(_FULL_DF) + "\n"
        with patch.object(backend, "_exec_with_status", return_value=(output, 82)):
            with pytest.raises(WorkspaceDiskFullError) as raised:
                backend._tmux_exec_checked(":", operation="reset stateless shell")
        assert raised.value.report.command_in_flight is True

    def test_terminal_cleanup_names_a_full_volume_too(self):
        backend = _backend()
        output = "srw-state-write-failed " + " ".join(_FULL_DF) + "\n"
        with (
            patch.object(backend, "_exec_with_status", return_value=(output, 82)),
            patch.object(backend, "_ensure_connected"),
        ):
            with pytest.raises(WorkspaceDiskFullError, match="exit code 82"):
                backend.exec_terminal_claim_resource(":")

    def test_reserve_marker_never_reaches_command_output(self):
        backend = _backend()
        output = "srw-disk-reserve-released " + " ".join(_FULL_DF) + "\nresult\n"
        with patch.object(backend, "_exec_with_status", return_value=(output, 0)):
            assert backend._tmux_exec_checked(":", operation="probe") == "result\n"
        assert backend.take_disk_full_notice() is not None

    @pytest.mark.parametrize("code", [73, 75, 78, 79, 80, 81])
    def test_fence_codes_stay_plain_unavailable(self, code):
        backend = _backend()
        output = "srw-state-write-failed " + " ".join(_FULL_DF) + "\n"
        with patch.object(backend, "_exec_with_status", return_value=(output, code)):
            with pytest.raises(WorkspaceUnavailableError) as raised:
                backend._tmux_exec_checked(":", operation="probe")
        assert type(raised.value) is WorkspaceUnavailableError

    def test_completion_payload_keeps_the_type_and_adds_the_usage(self):
        usage = WorkspaceDiskUsage.from_df(_FULL_DF)
        error = completion_error_payload(
            WorkspaceDiskFullError("full", usage=usage, command_in_flight=False)
        )["error"]
        # Old orchestrators route on the type alone and still hold the job.
        assert error["type"] == "workspace_unavailable"
        assert error["recoverable"] is True
        assert error["reason"] == WORKSPACE_DISK_FULL_REASON
        assert error["disk"] == usage.as_dict()
        assert error["command_in_flight"] is False
        assert WorkspaceDiskFullReport.from_error(error) == WorkspaceDiskFullReport(
            usage=usage, command_in_flight=False
        )


class TestShellToolNotice:
    def test_shell_result_leads_with_the_notice_once(self):
        from agent.tools.shell.shell_tools import _with_disk_full_notice

        class _Manager:
            notice = "⚠ Workspace disk is full."

            def take_disk_full_notice(self):
                notice, self.notice = self.notice, None
                return notice

        manager = _Manager()
        assert _with_disk_full_notice(manager, "Exit code: 0") == (
            "⚠ Workspace disk is full.\nExit code: 0"
        )
        assert _with_disk_full_notice(manager, "Exit code: 0") == "Exit code: 0"


class TestWireContract:
    def test_usage_parsing_rejects_what_df_cannot_mean(self):
        assert WorkspaceDiskUsage.from_df(("1", "2")) is None
        assert WorkspaceDiskUsage.from_df(("1", "x", "3")) is None
        no_inodes = WorkspaceDiskUsage.from_df(("1024", "1024", "0", "0", "0", "0"))
        assert no_inodes is not None and no_inodes.inodes_total is None
        assert WorkspaceDiskUsage.from_dict({"size_bytes": True}) is None
        assert (
            WorkspaceDiskUsage.from_dict(
                {"size_bytes": 1, "used_bytes": -1, "available_bytes": 0}
            )
            is None
        )
        reported = WorkspaceDiskUsage.from_dict(
            {
                "size_bytes": 10,
                "used_bytes": 10,
                "available_bytes": 0,
                "inodes_total": 5,
                "inodes_free": 9,
            }
        )
        assert reported is not None and reported.inodes_total is None

    def test_inode_exhaustion_is_full_and_says_so(self):
        usage = WorkspaceDiskUsage.from_df(
            ("10255636", "4000000", "6255636", "655360", "655360", "0")
        )
        assert usage is not None and usage.full
        assert usage.describe().startswith("655,360 of 655,360 files used; 3.8 GiB")

    def test_report_without_the_reason_is_not_disk_full(self):
        assert (
            WorkspaceDiskFullReport.from_error({"type": "workspace_unavailable"})
            is None
        )
        report = WorkspaceDiskFullReport.from_error(
            {"type": "workspace_unavailable", "reason": WORKSPACE_DISK_FULL_REASON}
        )
        # Unknown command state is treated as possibly running.
        assert report == WorkspaceDiskFullReport(usage=None, command_in_flight=True)

    def test_format_bytes(self):
        assert format_bytes(0) == "0 B"
        assert format_bytes(20 * 1024**3) == "20 GiB"
        assert format_bytes(512 * 1024**2) == "512 MiB"

    def test_actions_never_claim_a_lost_transport(self):
        report = WorkspaceDiskFullReport(
            usage=WorkspaceDiskUsage.from_df(_FULL_DF), command_in_flight=False
        )
        target = 20 * 1024**3
        actions = {
            "resumed": workspace_disk_full_action(
                report, WorkspaceVolumeGrowth("resized", target), resumed=True
            ),
            "grown_held": workspace_disk_full_action(
                report, WorkspaceVolumeGrowth("resized", target)
            ),
            "pending": workspace_disk_full_action(
                report, WorkspaceVolumeGrowth("pending", target)
            ),
            "used": workspace_disk_full_action(
                report, WorkspaceVolumeGrowth("used", target)
            ),
            "refused": workspace_disk_full_action(
                report, WorkspaceVolumeGrowth("refused", target, "HTTP 403")
            ),
            "none": workspace_disk_full_action(report),
        }
        for action in actions.values():
            assert "transport" not in action
            assert "(9.3 GiB of 9.8 GiB used, 0 B free)" in action
        assert actions["resumed"].endswith(
            "grew the volume to 20 GiB; "
            "workspace and checkpoints retained; "
            "the job resumes"
        )
        assert "explicit Resume required" in actions["grown_held"]
        assert "has not finished yet" in actions["pending"]
        assert "already grown once, to 20 GiB" in actions["used"]
        assert "growing the volume failed: HTTP 403" in actions["refused"]
        assert actions["none"].endswith("free space or grow the volume, then Resume")
