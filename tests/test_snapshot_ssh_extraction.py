"""Contract tests for streaming snapshot restore (memory-safe extraction) and
the capture-side honest accept gate.

Guards against regressing to "read the whole .tar.zst into RAM, then pass it as
``communicate(input=...)``" — the pattern that OOM-killed the orchestrator on
resume. The helper must stream the local tar file to the child process via its
stdin file descriptor. The global ``asyncio.create_subprocess_exec`` is patched,
so no real SSH is spawned.

Also guards the capture-side accept gate (``SnapshotService.capture_vm_snapshot``,
see knowledge-base/knowledge/features/workspace_durability_tiering.md §C1/C1b): a shell pipeline
(``tar | zstd``) only surfaces the LAST stage's exit code, so a truncated/failing
``tar`` upstream is masked and a partial archive gets accepted as good — unless
the remote command is rewritten to discriminate the two stages via ``PIPESTATUS``.

Also guards the extract-side ``pipefail`` guard (§C1/C1c): the restore pipeline
(``zstd -dc | byte limiter | tar ...``) already returns ``tar``'s (last stage's)
own exit code unchanged, but a ``zstd -dc`` decompression failure on a corrupt/truncated
archive is masked whenever ``tar`` still exits 0 — plain ``set -o pipefail``
(no PIPESTATUS discrimination) fixes this without altering tar's own rc
handling, including the benign full-extract tar rc==2 case.
"""

import asyncio
import base64
import hashlib
import os
import shlex
import shutil
import subprocess
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import paramiko
import pytest

from orchestrator.services.snapshot_service import (  # noqa: E402
    CREDENTIAL_EXCLUDE_PATTERNS,
    SnapshotService,
)
from orchestrator.services.ssh_helpers import (  # noqa: E402
    EXTRACT_HOME_REMOTE_CMD,
    EXTRACT_REMOTE_CMD,
    SSHPrivateKeyError,
    _read_stream_tail,
    build_agent_ssh_cmd,
    stream_extract_snapshot,
    wait_for_agent_ssh,
    workspace_private_key_fingerprint,
)

VALID_TEST_FINGERPRINT = "SHA256:" + ("A" * 43)


def _fake_proc(returncode=0, stderr=b"", stdout=b""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate = AsyncMock(return_value=(stdout, stderr))
    proc.wait = AsyncMock(return_value=returncode)
    proc.stdout.read = AsyncMock(side_effect=[stdout, b""])
    proc.stderr.read = AsyncMock(side_effect=[stderr, b""])
    return proc


class TestWorkspacePrivateKeyFingerprint:
    def test_valid_key_returns_only_public_fingerprint(self, tmp_path):
        key_path = tmp_path / "id_rsa"
        paramiko.RSAKey.generate(1024).write_private_key_file(str(key_path))

        fingerprint = workspace_private_key_fingerprint(str(key_path))

        assert fingerprint.startswith("SHA256:")
        assert "PRIVATE KEY" not in fingerprint

    def test_missing_key_is_a_safe_configuration_error(self, tmp_path):
        with pytest.raises(SSHPrivateKeyError, match="does not exist"):
            workspace_private_key_fingerprint(str(tmp_path / "missing"))


@pytest.fixture
def tar_file():
    """A small real file so the helper's open() yields a genuine file object."""
    with tempfile.NamedTemporaryFile(suffix=".tar.zst", delete=True) as tmp:
        tmp.write(b"\x28\xb5\x2f\xfd" + b"payload" * 64)
        tmp.flush()
        yield tmp.name


class TestBuildAgentSshCmd:
    def test_includes_options_host_and_remote_cmd(self):
        cmd = build_agent_ssh_cmd(
            "10.0.0.5", 2222, EXTRACT_REMOTE_CMD, key_path="/tmp/key"
        )
        assert cmd[0] == "ssh"
        assert "-i" in cmd and "/tmp/key" in cmd
        assert "StrictHostKeyChecking=no" in cmd
        assert "UserKnownHostsFile=/dev/null" in cmd
        assert "ConnectTimeout=10" in cmd
        assert "-p" in cmd and "2222" in cmd
        assert "agent-host@10.0.0.5" in cmd
        assert cmd[-1] == EXTRACT_REMOTE_CMD

    def test_no_key_omits_i_flag(self):
        cmd = build_agent_ssh_cmd("h", 22, EXTRACT_REMOTE_CMD, key_path="")
        assert "-i" not in cmd

    def test_probe_options_are_explicit(self):
        cmd = build_agent_ssh_cmd(
            "h",
            22,
            "true",
            key_path="/tmp/key",
            connect_timeout_s=3,
            batch_mode=True,
        )
        assert "ConnectTimeout=3" in cmd
        assert "BatchMode=yes" in cmd
        assert "IdentitiesOnly=yes" in cmd
        assert "PreferredAuthentications=publickey" in cmd
        assert cmd[-1] == "true"


class TestWaitForAgentSsh:
    @pytest.mark.asyncio
    async def test_success_returns_attempt_count(self):
        fake = _fake_proc(returncode=0)
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ready, attempts, error = await wait_for_agent_ssh(
                "10.0.0.9",
                22,
                deadline_s=1,
                connect_timeout_s=1,
                interval_s=0,
                key_path="/tmp/k",
            )

        assert ready is True
        assert attempts == 1
        assert error == ""

    @pytest.mark.asyncio
    async def test_failure_returns_last_error(self):
        fake = _fake_proc(returncode=255, stderr=b"ssh: connect timed out")
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ready, attempts, error = await wait_for_agent_ssh(
                "10.0.0.9",
                22,
                deadline_s=0,
                connect_timeout_s=1,
                interval_s=0,
                key_path="/tmp/k",
            )

        assert ready is False
        assert attempts == 1
        assert "timed out" in error

    @pytest.mark.asyncio
    async def test_missing_ssh_binary_fails_safely_without_retry(self):
        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=FileNotFoundError("ssh")),
        ):
            ready, attempts, error = await wait_for_agent_ssh(
                "10.0.0.9",
                22,
                deadline_s=30,
                connect_timeout_s=1,
                interval_s=1,
                key_path="/tmp/k",
            )

        assert ready is False
        assert attempts == 1
        assert error == "ssh readiness probe could not start: FileNotFoundError"

    @pytest.mark.asyncio
    async def test_pinned_probe_enforces_selected_host_key(self):
        fake = _fake_proc(returncode=0)
        scan = AsyncMock(return_value=("host ssh-ed25519 AAAAtest", b""))
        with (
            patch(
                "orchestrator.services.ssh_helpers._scan_pinned_host_key",
                new=scan,
            ),
            patch(
                "asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=fake),
            ) as execute,
        ):
            ready, attempts, error = await wait_for_agent_ssh(
                "10.0.0.9",
                22,
                deadline_s=1,
                connect_timeout_s=1,
                interval_s=0,
                key_path="/tmp/k",
                expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
            )

        assert (ready, attempts, error) == (True, 1, "")
        scan.assert_awaited_once_with(
            "10.0.0.9", 22, VALID_TEST_FINGERPRINT, timeout_s=1
        )
        argv = execute.await_args.args
        assert "StrictHostKeyChecking=yes" in argv
        assert "UserKnownHostsFile=/dev/null" not in argv

    @pytest.mark.asyncio
    async def test_pinned_probe_rejects_mismatch_before_authentication(self):
        mismatch = b"SSH host-key fingerprint mismatch"
        execute = AsyncMock()
        with (
            patch(
                "orchestrator.services.ssh_helpers._scan_pinned_host_key",
                new=AsyncMock(return_value=(None, mismatch)),
            ),
            patch("asyncio.create_subprocess_exec", new=execute),
        ):
            ready, attempts, error = await wait_for_agent_ssh(
                "10.0.0.9",
                22,
                deadline_s=0,
                connect_timeout_s=1,
                interval_s=0,
                key_path="/tmp/k",
                expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
            )

        assert ready is False
        assert attempts == 1
        assert error == mismatch.decode()
        execute.assert_not_awaited()


class TestStreamExtractSnapshot:
    @pytest.mark.asyncio
    async def test_rejects_compressed_archive_over_cap_before_ssh(
        self, tar_file, monkeypatch
    ):
        create = AsyncMock(side_effect=AssertionError("size cap precedes SSH"))
        monkeypatch.setenv("SNAPSHOT_MAX_SIZE_GB", "1")
        with (
            patch(
                "orchestrator.services.ssh_helpers.os.path.getsize",
                return_value=(1024 * 1024 * 1024) + 1,
            ),
            patch("asyncio.create_subprocess_exec", new=create),
        ):
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9", 22, tar_file, key_path="/tmp/k"
            )

        assert rc == 125
        assert b"compressed-size cap" in stderr
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_streams_file_as_stdin_not_bytes(self, tar_file):
        fake = _fake_proc(returncode=0)
        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
        ) as mock_exec:
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9", 22, tar_file, key_path="/tmp/k"
            )

        assert rc == 0
        assert stderr == b""

        # stdin must be a streamed file object, never the file's bytes —
        # this is the regression guard against re-introducing f.read().
        stdin = mock_exec.call_args.kwargs["stdin"]
        assert not isinstance(stdin, (bytes, bytearray, memoryview))
        assert hasattr(stdin, "read") and hasattr(stdin, "fileno")

        # The child is awaited directly while stderr is drained into a bounded
        # tail; communicate(input=...) would buffer the archive in RAM.
        fake.communicate.assert_not_awaited()
        fake.wait.assert_awaited_once_with()
        assert mock_exec.call_args.kwargs["stdout"] == asyncio.subprocess.DEVNULL

        # argv contract
        argv = mock_exec.call_args.args
        assert argv[0] == "ssh"
        assert "StrictHostKeyChecking=no" in argv
        assert "agent-host@10.0.0.9" in argv
        remote_argv = shlex.split(argv[-1])
        assert remote_argv[:2] == ["flock", "-w"]
        assert remote_argv[3] == "/tmp/.srw-vm-remote-operation.lock"
        assert "timeout" in remote_argv
        assert remote_argv[-1] == EXTRACT_REMOTE_CMD
        # Verify extract command includes xattrs/acls for overlay whiteout
        # round-trip, wrapped in the `pipefail` guard that surfaces a masked
        # zstd decompression failure on restore (C1c).
        extract_body = shlex.split(EXTRACT_REMOTE_CMD)[2]
        assert extract_body.startswith("set -o pipefail; zstd -dc | python3 -c ")
        assert "limit=42949672960" in extract_body
        assert "| tar --no-same-owner --no-same-permissions" in extract_body
        assert '--xattrs-include="*" --acls -xf - -C /' in extract_body

    @pytest.mark.asyncio
    async def test_returns_rc_and_stderr_on_failure(self, tar_file):
        fake = _fake_proc(returncode=1, stderr=b"tar: short read")
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            rc, stderr = await stream_extract_snapshot(
                "h", 22, tar_file, key_path="/tmp/k"
            )
        assert rc == 1
        assert stderr == b"tar: short read"

    @pytest.mark.asyncio
    async def test_strict_extract_pins_scanned_key_and_uses_pipefail(self, tar_file):
        key_blob = b"provisioner-attested-ed25519-host-key"
        encoded = base64.b64encode(key_blob).decode("ascii")
        fingerprint = "SHA256:" + base64.b64encode(
            hashlib.sha256(key_blob).digest()
        ).decode("ascii").rstrip("=")
        scan = _fake_proc(stdout=f"[10.0.0.9]:30022 ssh-ed25519 {encoded}\n".encode())
        extract = _fake_proc()

        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=[scan, extract]),
        ) as mock_exec:
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9",
                30022,
                tar_file,
                key_path="/tmp/k",
                expected_host_key_fingerprint=fingerprint,
                require_pipefail=True,
            )

        assert (rc, stderr) == (0, b"")
        scan_argv = mock_exec.await_args_list[0].args
        assert scan_argv[:2] == ("ssh-keyscan", "-T")
        assert "30022" in scan_argv
        extract_argv = mock_exec.await_args_list[1].args
        assert (
            mock_exec.await_args_list[1].kwargs["stdout"] == asyncio.subprocess.DEVNULL
        )
        assert "StrictHostKeyChecking=yes" in extract_argv
        known_hosts = next(
            value for value in extract_argv if value.startswith("UserKnownHostsFile=")
        )
        assert known_hosts != "UserKnownHostsFile=/dev/null"
        remote_argv = shlex.split(extract_argv[-1])
        assert remote_argv[:2] == ["flock", "-w"]
        assert remote_argv[3] == "/tmp/.srw-vm-remote-operation.lock"
        assert "timeout" in remote_argv
        assert remote_argv[-5:-1] == ["bash", "-o", "pipefail", "-c"]
        assert "zstd -dc | python3 -c" in remote_argv[-1]
        assert "limit=42949672960" in remote_argv[-1]
        assert "--no-same-owner" in remote_argv[-1]

    @pytest.mark.asyncio
    async def test_strict_extract_cancellation_kills_and_reaps_ssh_child(
        self, tar_file
    ):
        wait_calls = 0

        async def _cancel_then_reap():
            nonlocal wait_calls
            wait_calls += 1
            if wait_calls == 1:
                raise asyncio.CancelledError
            return 0

        extract = _fake_proc()
        extract.returncode = None
        extract.wait = AsyncMock(side_effect=_cancel_then_reap)
        extract.kill = MagicMock()

        with (
            patch(
                "orchestrator.services.ssh_helpers._scan_pinned_host_key",
                new=AsyncMock(return_value=("host ssh-ed25519 key", b"")),
            ),
            patch(
                "asyncio.create_subprocess_exec", new=AsyncMock(return_value=extract)
            ),
        ):
            with pytest.raises(asyncio.CancelledError):
                await stream_extract_snapshot(
                    "10.0.0.9",
                    30022,
                    tar_file,
                    key_path="/tmp/k",
                    expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
                    require_pipefail=True,
                )

        extract.terminate.assert_called_once_with()
        extract.kill.assert_not_called()
        assert extract.wait.await_count >= 1

    @pytest.mark.asyncio
    async def test_strict_extract_timeout_kills_and_reaps_ssh_child(
        self, tar_file, monkeypatch
    ):
        wait_calls = 0

        async def _blocked_communicate():
            nonlocal wait_calls
            wait_calls += 1
            if wait_calls == 1:
                await asyncio.sleep(3600)
            return 0

        extract = _fake_proc()
        extract.returncode = None
        extract.wait = AsyncMock(side_effect=_blocked_communicate)
        extract.kill = MagicMock()
        monkeypatch.setenv("STATELESS_SNAPSHOT_RESTORE_TIMEOUT_S", "0.01")

        with (
            patch(
                "orchestrator.services.ssh_helpers._scan_pinned_host_key",
                new=AsyncMock(return_value=("host ssh-ed25519 key", b"")),
            ),
            patch(
                "asyncio.create_subprocess_exec", new=AsyncMock(return_value=extract)
            ),
        ):
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9",
                30022,
                tar_file,
                key_path="/tmp/k",
                expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
                require_pipefail=True,
            )

        assert rc == 124
        assert b"timed out" in stderr
        extract.terminate.assert_called_once_with()
        extract.kill.assert_not_called()
        assert extract.wait.await_count >= 1

    @pytest.mark.asyncio
    async def test_strict_extract_rejects_host_key_mismatch_before_bytes(
        self, tar_file
    ):
        encoded = base64.b64encode(b"different-host-key").decode("ascii")
        scan = _fake_proc(stdout=f"[10.0.0.9]:30022 ssh-ed25519 {encoded}\n".encode())

        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=scan)
        ) as mock_exec:
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9",
                30022,
                tar_file,
                key_path="/tmp/k",
                expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
                require_pipefail=True,
            )

        assert rc == 255
        assert b"did not match" in stderr
        assert mock_exec.await_count == 1

    @pytest.mark.asyncio
    async def test_cancelled_host_key_scan_kills_and_reaps_child(self, tar_file):
        scan = _fake_proc()
        scan.returncode = None
        scan.stdout.read = AsyncMock(side_effect=asyncio.CancelledError)
        scan.kill = MagicMock()
        scan.terminate = MagicMock()
        scan.wait = AsyncMock(return_value=0)

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=scan)):
            with pytest.raises(asyncio.CancelledError):
                await stream_extract_snapshot(
                    "10.0.0.9",
                    30022,
                    tar_file,
                    key_path="/tmp/k",
                    expected_host_key_fingerprint=VALID_TEST_FINGERPRINT,
                    require_pipefail=True,
                )

        scan.terminate.assert_called_once_with()
        scan.kill.assert_not_called()
        assert scan.wait.await_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fingerprint",
        [
            None,
            "",
            "SHA256:",
            "md5:abc",
            "SHA256:bad value",
            "SHA256:" + ("A" * 42),
            "SHA256:" + ("A" * 42) + "=",
            "SHA256:" + ("A" * 42) + "_",
        ],
    )
    async def test_strict_extract_refuses_missing_or_malformed_pin_before_ssh(
        self, tar_file, fingerprint
    ):
        create = AsyncMock(side_effect=AssertionError("strict pin precedes SSH"))
        with patch("asyncio.create_subprocess_exec", new=create):
            rc, stderr = await stream_extract_snapshot(
                "10.0.0.9",
                30022,
                tar_file,
                key_path="/tmp/k",
                expected_host_key_fingerprint=fingerprint,
                require_pipefail=True,
            )

        assert rc == 255
        assert b"pinned SSH host key" in stderr
        create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_subprocess_stderr_tail_is_bounded(self):
        stream = MagicMock()
        stream.read = AsyncMock(
            side_effect=[b"a" * 40000, b"b" * 40000, b"c" * 40000, b""]
        )

        tail = await _read_stream_tail(stream, limit=65536)

        assert len(tail) == 65536
        assert tail == (b"b" * 25536) + (b"c" * 40000)


class TestExtractRemoteCmdPipefail:
    """Extract-side ``set -o pipefail`` guard (§C1c).

    Restore runs ``zstd -dc | byte limiter | tar ...`` (see ``EXTRACT_REMOTE_CMD`` /
    ``EXTRACT_HOME_REMOTE_CMD`` above). A shell pipeline only reports the
    LAST stage's exit code; ``tar`` is already the last stage here, so
    today's pipeline correctly returns tar's own rc — but a ``zstd -d``
    failure on a corrupt/truncated archive is invisible whenever ``tar``
    still exits 0 (e.g. it received a short or empty stream and didn't
    itself error), so the restore is reported as a success on a partial or
    empty extract. The middle stage additionally enforces an uncompressed-byte
    cap before tar can write the stream.

    Unlike C1b's capture pipeline (``tar | zstd``, zstd last, where tar's
    benign rc==1 "file changed" warning had to be tolerated via a
    PIPESTATUS-discriminating rewrite), here plain ``set -o pipefail`` is
    the correct, minimal fix: it only adds "an earlier stage failing also
    fails the pipeline" — it does not touch tar's own rc handling,
    including the benign full-extract tar rc==2 (see the comment above
    ``EXTRACT_REMOTE_CMD``). No PIPESTATUS, no consumer changes.
    """

    def test_both_constants_are_wrapped_in_bash_c_pipefail(self):
        for cmd in (EXTRACT_REMOTE_CMD, EXTRACT_HOME_REMOTE_CMD):
            assert cmd.startswith("bash -c 'set -o pipefail; ")
            body = shlex.split(cmd)[2]
            assert "zstd -dc | python3 -c" in body
            assert "limit=42949672960" in body
            assert "| tar --no-same-owner --no-same-permissions" in body
            assert (
                subprocess.run(["bash", "-n", "-c", cmd], check=False).returncode == 0
            )

    def test_extract_remote_cmd_keeps_literal_star_and_valid_quoting(self):
        body = shlex.split(EXTRACT_REMOTE_CMD)[2]
        assert '--xattrs-include="*"' in body
        assert body.endswith("-C /")

    def test_extract_home_remote_cmd_has_valid_quoting(self):
        body = shlex.split(EXTRACT_HOME_REMOTE_CMD)[2]
        assert body.endswith("-C / home/agent-host")

    def test_pipefail_surfaces_masked_zstd_failure_under_real_bash(self):
        """Runs the actual pipeline SHAPE (``set -o pipefail; <stage0> |
        <stage1>``) under real bash with synthetic stage exit codes standing
        in for zstd (stage 0) and tar (stage 1) — mocked-subprocess tests
        can't catch shell bugs (the C1b lesson at
        ``test_pipestatus_discrimination_tail_maps_stage_exits_correctly``
        above), so this proves bash's real ``pipefail`` semantics instead of
        trusting the string shape asserted above.
        """
        if shutil.which("bash") is None:
            pytest.skip("bash not available")

        def run(zstd_rc: int, tar_rc: int, *, pipefail: bool) -> int:
            prefix = "set -o pipefail; " if pipefail else ""
            result = subprocess.run(
                ["bash", "-c", f"{prefix}( exit {zstd_rc} ) | ( exit {tar_rc} )"],
                capture_output=True,
                text=True,
            )
            return result.returncode

        # zstd succeeds: tar's own rc passes through completely unchanged
        # with pipefail enabled — including the benign full-extract tar
        # rc==2 (see class docstring). This is the "must stay exactly as it
        # is today" guarantee from the design doc.
        assert run(0, 0, pipefail=True) == 0
        assert run(0, 2, pipefail=True) == 2

        # zstd fails (the masked corrupt-archive case): the pipeline must no
        # longer report success regardless of tar's own rc. The exact
        # surfaced code is not the contract here — only that it stops being
        # a false 0 / false benign-2.
        assert run(1, 0, pipefail=True) != 0
        assert run(1, 2, pipefail=True) != 0

        # The load-bearing contrast proving `pipefail` is what matters: the
        # very same (zstd=1, tar=0) masking case reports SUCCESS without
        # pipefail (today's bug: a corrupt archive's zstd failure hidden by
        # tar's benign exit 0)...
        assert run(1, 0, pipefail=False) == 0
        # ...and only fails once pipefail is enabled — this is exactly the
        # fix C1c makes.
        assert run(1, 0, pipefail=True) != 0


def _fake_capture_proc(returncode=0, chunks=(b"tarball-bytes",), stderr=b""):
    """Fake asyncio subprocess for the capture-side SSH ``tar | zstd`` pipeline.

    ``stdout.read()`` yields ``chunks`` in order, then an empty ``bytes`` to
    signal EOF — mirroring the 1 MB chunked read loop in
    ``SnapshotService.capture_vm_snapshot``. ``returncode`` models the single
    honest exit code the remote ``bash -c`` wrapper computes from
    ``PIPESTATUS`` (0 clean, 1 tar-warned, 2 fatal), not a real shell
    pipeline's raw last-stage code.
    """
    proc = MagicMock()
    proc.returncode = returncode
    proc.stdout = MagicMock()
    proc.stdout.read = AsyncMock(side_effect=[*chunks, b""])
    proc.stderr = MagicMock()
    proc.stderr.read = AsyncMock(side_effect=[stderr, b""])
    proc.wait = AsyncMock(return_value=None)
    return proc


class TestCaptureVmSnapshotAcceptGate:
    """Capture's accept gate must be honest about a masked pipeline failure.

    A shell pipeline (``tar | zstd``) only reports the LAST stage's exit code,
    so a truncated/failing ``tar`` upstream is silently masked and a partial
    archive gets accepted as good. But tar's rc==1 ("file changed as we read
    it") is a routine warning on a live workspace, not a failure — rejecting
    every nonzero rc would fail capture constantly. The honest rule: accept
    rc in {0, 1}; reject rc >= 2 or an empty byte stream. See
    knowledge-base/knowledge/features/workspace_durability_tiering.md §C1 (C1b block).
    """

    @pytest.fixture
    def service(self):
        """SnapshotService with S3 "available" and its downstream calls
        (context updates, env-info collection, upload) mocked out, so each
        test isolates the accept-gate branch without touching S3/DB/SSH.
        """
        svc = SnapshotService()
        svc._available = True
        svc._set_snapshot_context = AsyncMock()
        svc._collect_environment_info = AsyncMock(return_value={})
        svc.upload_snapshot = AsyncMock(return_value=True)
        return svc

    @staticmethod
    def _statuses(mock_set_snapshot_context):
        """Every ``status`` value passed to the mocked ``_set_snapshot_context``."""
        return [
            call.args[1].get("status")
            for call in mock_set_snapshot_context.call_args_list
        ]

    @pytest.mark.asyncio
    async def test_remote_command_is_bash_c_with_pipestatus_discrimination(
        self, service
    ):
        fake = _fake_capture_proc(returncode=0)
        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
        ) as mock_exec:
            await service.capture_vm_snapshot("job-cmd", "192.0.2.10", 2222)

        remote_cmd = mock_exec.call_args.args[-1]
        assert remote_cmd.startswith("bash -c '")
        assert "tar --xattrs" in remote_cmd
        assert "| zstd -1 -T0" in remote_cmd
        # PIPESTATUS must be snapshotted into an array in ONE command before
        # anything reads it (a bare `__t=${PIPESTATUS[0]}` assignment is
        # itself a simple command and immediately resets PIPESTATUS, so a
        # second assignment reading index 1 would see it already clobbered —
        # see test_pipestatus_discrimination_tail_maps_stage_exits_correctly
        # for the runtime-behavior regression guard).
        assert '__ps=("${PIPESTATUS[@]}")' in remote_cmd
        assert "${__ps[0]}" in remote_cmd
        assert "${__ps[1]}" in remote_cmd
        assert "exit 2" in remote_cmd
        assert "exit 1" in remote_cmd
        assert "exit 0" in remote_cmd
        # The `-c` body is single-quoted: exactly the opening and closing
        # quote should exist anywhere in the command — a stray single quote
        # from an exclude/include pattern would prematurely close the
        # argument and break the remote shell.
        assert remote_cmd.count("'") == 2

    @pytest.mark.asyncio
    async def test_rc_2_is_rejected_as_capture_failed(self, service):
        fake = _fake_capture_proc(returncode=2, stderr=b"tar: fatal error")
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            result = await service.capture_vm_snapshot("job-rc2", "192.0.2.10", 2222)

        assert result is False
        assert "capture_failed" in self._statuses(service._set_snapshot_context)
        service.upload_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rc_1_tar_warning_is_accepted(self, service, caplog):
        fake = _fake_capture_proc(returncode=1)
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            with caplog.at_level("WARNING"):
                result = await service.capture_vm_snapshot(
                    "job-rc1", "192.0.2.10", 2222
                )

        assert result is True
        service.upload_snapshot.assert_awaited_once()
        assert "capture_failed" not in self._statuses(service._set_snapshot_context)
        assert any(
            "rc=1" in record.getMessage() or "files changed" in record.getMessage()
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_rc_0_clean_is_accepted(self, service):
        fake = _fake_capture_proc(returncode=0)
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            result = await service.capture_vm_snapshot("job-rc0", "192.0.2.10", 2222)

        assert result is True
        service.upload_snapshot.assert_awaited_once()
        assert "capture_failed" not in self._statuses(service._set_snapshot_context)

    @pytest.mark.asyncio
    async def test_empty_stream_is_rejected_even_with_rc_0(self, service):
        fake = _fake_capture_proc(returncode=0, chunks=())
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            result = await service.capture_vm_snapshot("job-empty", "192.0.2.10", 2222)

        assert result is False
        assert "capture_failed" in self._statuses(service._set_snapshot_context)
        service.upload_snapshot.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pipestatus_discrimination_tail_maps_stage_exits_correctly(
        self, service
    ):
        """Executes the actual shipped PIPESTATUS-reading shell logic under
        real bash with synthetic (tar_rc, zstd_rc) stage exits and asserts
        the 0/1/2 mapping end to end.

        Every other test in this class mocks ``create_subprocess_exec`` with
        a hardcoded ``returncode`` — the shell arithmetic that is supposed to
        *compute* that code has ZERO runtime coverage there. That blind spot
        is real: a bare assignment (``__t=${PIPESTATUS[0]}``) is itself a
        simple command and immediately resets ``PIPESTATUS`` to its own exit
        status, so a second assignment reading index 1 would silently see it
        already clobbered — re-masking every zstd failure while every
        mock-returncode test above stays green. This test extracts the real
        tail from the command the service actually constructs and runs it
        against real bash, so a regression to the broken form fails here.
        """
        if shutil.which("bash") is None:
            pytest.skip("bash not available")

        fake = _fake_capture_proc(returncode=0)
        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
        ) as mock_exec:
            await service.capture_vm_snapshot("job-tail", "192.0.2.10", 2222)

        remote_cmd = mock_exec.call_args.args[-1]
        assert remote_cmd.startswith("bash -c '")
        assert remote_cmd.endswith("'")
        body = remote_cmd[len("bash -c '") : -1]
        # Isolate the discrimination tail from the tar|zstd pipeline that
        # precedes it: everything after "| zstd -1 -T0; ". Anchored on the
        # pipeline's own tail (present regardless of how the discrimination
        # logic itself reads PIPESTATUS) rather than on a variable name from
        # the current fix, so a regression to a *different* broken form of
        # the tail still gets extracted and exercised instead of vanishing
        # into a substring-not-found error.
        anchor = "| zstd -1 -T0; "
        tail = body[body.index(anchor) + len(anchor) :]

        # (tar_rc, zstd_rc) -> honest mapped exit code (0 clean, 1 tar
        # warned/accept, 2 fatal) per knowledge-base/knowledge/features/workspace_durability_
        # tiering.md §C1 (C1b): accept tar rc in {0,1}; reject tar rc>=2 or
        # any zstd failure — zstd failing must dominate regardless of tar's
        # own code.
        cases = [
            (0, 0, 0),
            (1, 0, 1),
            (2, 0, 2),
            (0, 1, 2),
            (0, 5, 2),
            (1, 1, 2),
        ]
        for tar_rc, zstd_rc, expected in cases:
            result = subprocess.run(
                ["bash", "-c", f"( exit {tar_rc} ) | ( exit {zstd_rc} ); {tail}"],
                capture_output=True,
                text=True,
            )
            assert result.returncode == expected, (
                f"tar_rc={tar_rc} zstd_rc={zstd_rc}: expected exit {expected}, "
                f"got {result.returncode} (stderr={result.stderr!r})"
            )
            # The correct form never trips a `[: integer expected` usage
            # error (the symptom of PIPESTATUS having already been clobbered).
            assert "integer expected" not in result.stderr

    @pytest.mark.asyncio
    async def test_capture_excludes_ext4_lost_found(self, service):
        """The capture tar must exclude the ext4 ``lost+found`` artifact.

        An ext4-formatted PVC mounts a root-owned 0700 ``lost+found`` at the
        volume root — which, for a session/job workspace, IS
        ``/home/agent-host``. The capture tar runs as the unprivileged
        ``agent-host`` SSH user and cannot open it, so without this exclude the
        whole ``tar`` exits rc>=2 and the C1b accept gate (correctly) rejects
        the archive — breaking EVERY PVC-backed capture, so idle-suspend can
        never complete and reclaim-on-idle can never fire. Confirmed on the dev
        cluster: ``tar: /home/agent-host/lost+found: Cannot open: Permission
        denied`` was the sole cause of a real capture rc=2. See
        knowledge-base/knowledge/features/workspace_durability_tiering.md §C1.
        """
        fake = _fake_capture_proc(returncode=0)
        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
        ) as mock_exec:
            await service.capture_vm_snapshot("job-lostfound", "192.0.2.10", 2222)

        remote_cmd = mock_exec.call_args.args[-1]
        assert "--exclude=*/lost+found" in remote_cmd

    def test_lost_found_exclude_lets_real_tar_skip_unreadable_dir(self, tmp_path):
        """A real ``tar`` over a workspace containing an unreadable subdir exits
        rc>=2 (fatal, which the C1b gate rejects) UNLESS that subdir is
        excluded — the exact ext4 ``lost+found`` situation. Mocked-subprocess
        tests can't catch this (the C1b lesson): the exclude only helps if
        tar's real argument matching skips the entry before ever opening it,
        so prove it against real tar rather than trusting the string shape.
        """
        if shutil.which("tar") is None:
            pytest.skip("tar not available")
        if os.geteuid() == 0:
            pytest.skip("root bypasses filesystem permission checks")

        home = tmp_path / "home"
        home.mkdir()
        (home / "keep.txt").write_text("workspace data\n")
        lost = home / "lost+found"
        lost.mkdir()
        (lost / "orphan").write_text("x")
        os.chmod(lost, 0o000)  # mimic the root-owned 0700 dir: owner can't read
        try:

            def run_tar(*excludes: str) -> int:
                return subprocess.run(
                    ["tar", "-cf", os.devnull, *excludes, "-C", str(tmp_path), "home"],
                    capture_output=True,
                    text=True,
                ).returncode

            # Unreadable dir present -> tar cannot open it -> fatal rc>=2.
            assert run_tar() >= 2
            # Excluded (the fix) -> tar never opens it -> clean exit.
            assert run_tar("--exclude=*/lost+found") == 0
        finally:
            os.chmod(lost, 0o755)  # allow tmp_path teardown to remove it

    @pytest.mark.asyncio
    async def test_capture_excludes_connector_credentials(self, service):
        """Connector credentials in the workspace home never reach a snapshot.

        Env connector files, the managed-repository ssh-agent namespace and
        the legacy external-repository key files are rewritten from the
        dispatch payload on every attach; a snapshot must not become a second
        durable copy of them. The user's own ``~/.ssh`` stays in.
        """
        fake = _fake_capture_proc(returncode=0)
        with patch(
            "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
        ) as mock_exec:
            await service.capture_vm_snapshot("job-credentials", "192.0.2.10", 2222)

        remote_cmd = mock_exec.call_args.args[-1]
        assert "--exclude=.srw-credentials " in remote_cmd
        assert "--exclude=.ssh/srw-managed " in remote_cmd
        assert "--exclude=.ssh/repo_* " in remote_cmd
        for pattern in CREDENTIAL_EXCLUDE_PATTERNS:
            assert f"{pattern} " in remote_cmd
        assert "--exclude=.ssh " not in remote_cmd
        assert remote_cmd.count("'") == 2

    def test_capture_command_omits_credentials_from_a_real_archive(self, tmp_path):
        """Run the real non-strict capture command against a fixture home.

        Proves GNU tar's own matching drops exactly the three credential
        locations and keeps the rest of ``~/.ssh``, rather than trusting the
        string shape of the exclude list.
        """
        for tool in ("tar", "zstd", "bash"):
            if shutil.which(tool) is None:
                pytest.skip(f"{tool} not available")
        # The capture excludes /tmp/*; keep the fixture under the home
        # directory like the production root.
        with tempfile.TemporaryDirectory(
            prefix="snapshot-credentials-", dir=os.path.expanduser("~")
        ) as root:
            home = os.path.join(root, "agent-host")
            fixture = {
                ".srw-credentials/0123abcd.sh": "export C0=env-secret\n",
                ".ssh/srw-managed/config.d/srw-repo-x.conf": "Host srw-repo-x\n",
                ".ssh/srw-managed/agents/srw-repo-x.state": "pid=1\n",
                ".ssh/repo_github": "legacy-private-key\n",
                ".ssh/config": "Host example\n",
                ".ssh/id_ed25519": "user-private-key\n",
                ".ssh/known_hosts": "example ssh-ed25519 AAAA\n",
                "workspace/notes.md": "kept\n",
                ".cache/srw/rclone/t1/workspace/rclone.conf": "pass = obscured\n",
                ".cache/srw/rclone/t1/workspace/bearer.token": "bearer\n",
                ".cache/srw/rclone/t1/workspace/bearer.token.new.0a1b": "bearer\n",
                ".cache/srw/rclone/t1/workspace/.bearer.token.Xy12AbCd": "bearer\n",
                ".cache/srw/rclone/t1/workspace/bearer-helper.sh": "exec cat token\n",
                ".cache/srw/rclone/t1/workspace/vfs-cache/vfs/r/notes.md": "cached\n",
            }
            for relative, content in fixture.items():
                path = os.path.join(home, relative)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as handle:
                    handle.write(content)

            async def capture() -> str:
                fake = _fake_capture_proc(returncode=0)
                svc = SnapshotService()
                svc._available = True
                svc._set_snapshot_context = AsyncMock()
                svc._collect_environment_info = AsyncMock(return_value={})
                svc.upload_snapshot = AsyncMock(return_value=True)
                with patch(
                    "asyncio.create_subprocess_exec",
                    new=AsyncMock(return_value=fake),
                ) as mock_exec:
                    await svc.capture_vm_snapshot("job-real-tar", "192.0.2.10", 2222)
                return mock_exec.call_args.args[-1]

            remote_cmd = asyncio.run(capture())
            local_cmd = remote_cmd.replace(
                " /home/agent-host/ /usr/local/ ", f" {home}/ "
            )
            assert local_cmd != remote_cmd
            archive = os.path.join(tmp_path, "env.tar.zst")
            with open(archive, "wb") as output:
                completed = subprocess.run(
                    ["bash", "-c", local_cmd], stdout=output, check=False
                )
            assert completed.returncode in (0, 1)
            listing = subprocess.run(
                ["bash", "-c", f"zstd -dc -- {shlex.quote(archive)} | tar -tf -"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.splitlines()

        assert not [m for m in listing if "/.srw-credentials" in m]
        assert not [m for m in listing if "/.ssh/srw-managed" in m]
        assert not [m for m in listing if "/.ssh/repo_" in m]
        assert not [m for m in listing if m.endswith("/rclone.conf")]
        assert not [m for m in listing if "bearer.token" in m]
        for kept in (
            ".ssh/config",
            ".ssh/id_ed25519",
            ".ssh/known_hosts",
            ".cache/srw/rclone/t1/workspace/bearer-helper.sh",
            ".cache/srw/rclone/t1/workspace/vfs-cache/vfs/r/notes.md",
        ):
            assert any(m.endswith(f"agent-host/{kept}") for m in listing), kept
        assert any(m.endswith("agent-host/workspace/notes.md") for m in listing)
