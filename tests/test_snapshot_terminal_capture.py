from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import subprocess
import tempfile
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.snapshot_service import (
    CREDENTIAL_EXCLUDE_PATTERNS,
    SnapshotService,
    _read_stream_tail,
    _snapshot_tar_pipeline,
)


def _capture_process(*, stdout: list[bytes], returncode: int) -> MagicMock:
    process = MagicMock()
    process.stdout.read = AsyncMock(side_effect=[*stdout, b""])
    process.stderr.read = AsyncMock(side_effect=[b"producer failed", b""])
    process.wait = AsyncMock(return_value=returncode)
    process.returncode = returncode
    return process


def _host_key_scan_process() -> tuple[MagicMock, str]:
    key = b"terminal-workspace-host-key"
    encoded = base64.b64encode(key).decode("ascii")
    fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(key).digest()).decode(
        "ascii"
    ).rstrip("=")
    process = MagicMock()
    process.stdout.read = AsyncMock(
        side_effect=[f"10.0.0.5 ssh-ed25519 {encoded}\n".encode(), b""]
    )
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    process.returncode = 0
    return process, fingerprint


@pytest.mark.asyncio
async def test_capture_revalidates_authority_before_context_and_ssh() -> None:
    service = SnapshotService()
    service._available = True
    service._snapshot_is_available = AsyncMock(return_value=False)
    service._set_snapshot_context = AsyncMock()
    service.upload_snapshot = AsyncMock(return_value=True)
    authority = AsyncMock(side_effect=[True, True, False])
    create = AsyncMock(side_effect=AssertionError("lost authority must precede SSH"))

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=create,
    ):
        captured = await service.capture_vm_snapshot(
            job_id="job-authority",
            ssh_host="10.0.0.5",
            ssh_port=22,
            capture_authority=authority,
        )

    assert captured is False
    assert authority.await_count == 3
    service._set_snapshot_context.assert_awaited_once_with(
        "job-authority", {"status": "capturing"}, entity_type="jobs"
    )
    create.assert_not_awaited()
    service.upload_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_environment_probes_revalidate_before_each_ssh_connection() -> None:
    service = SnapshotService()
    authority = AsyncMock(side_effect=[True, False])
    process = MagicMock(returncode=0)
    spawn = AsyncMock(return_value=process)

    with (
        patch(
            "orchestrator.services.snapshot_service.create_owned_subprocess_exec",
            new=spawn,
        ),
        patch(
            "orchestrator.services.snapshot_service.communicate_bounded",
            new=AsyncMock(return_value=(b"package==1\n", b"")),
        ),
    ):
        info = await service._collect_environment_info(
            "10.0.0.5",
            22,
            capture_authority=authority,
        )

    assert info == {"pip_freeze": ["package==1"]}
    assert authority.await_count == 2
    assert spawn.await_count == 1


def test_orchestrator_runtime_images_include_snapshot_validator() -> None:
    repository = Path(__file__).resolve().parents[1]
    for relative_path in (
        "docker/Dockerfile.orchestrator",
        "docker/Dockerfile.orchestrator.dev",
    ):
        dockerfile = (repository / relative_path).read_text(encoding="utf-8")
        runtime_stage = dockerfile.rsplit("FROM python:3.11-slim", 1)[1]
        runtime_packages = runtime_stage.split(
            "RUN apt-get update && apt-get install -y --no-install-recommends", 1
        )[1].split("&& rm -rf /var/lib/apt/lists/*", 1)[0]

        assert "\n    zstd \\" in runtime_packages, (
            f"{relative_path} must install zstd in its runtime stage"
        )


@pytest.mark.asyncio
async def test_strict_capture_rejects_partial_archive_from_failed_tar() -> None:
    service = SnapshotService()
    service._available = True
    service.upload_snapshot = AsyncMock(return_value=True)
    failed = _capture_process(stdout=[b"partial-zstd-output"], returncode=23)
    scan, fingerprint = _host_key_scan_process()

    with (
        patch.object(
            SnapshotService, "_collect_environment_info", AsyncMock(return_value={})
        ),
        patch(
            "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=[scan, failed]),
        ),
    ):
        captured = await service.capture_vm_snapshot(
            job_id="terminal-thread",
            ssh_host="10.0.0.5",
            ssh_port=30022,
            source_type="pod",
            entity_type="threads",
            expected_host_key_fingerprint=fingerprint,
            strict_terminal=True,
        )

    assert captured is False
    service.upload_snapshot.assert_not_awaited()
    assert failed.stderr.read.await_count == 2


@pytest.mark.asyncio
async def test_strict_archive_validation_preserves_validator_stderr() -> None:
    service = SnapshotService()
    service._available = True
    service._set_snapshot_context = AsyncMock()
    service.upload_snapshot = AsyncMock(return_value=True)
    capture = _capture_process(stdout=[b"complete-archive"], returncode=0)
    verify = MagicMock()
    verify.stderr.read = AsyncMock(
        side_effect=[b"bash: line 1: zstd: command not found\n", b""]
    )
    verify.wait = AsyncMock(return_value=127)
    verify.returncode = 127
    scan, fingerprint = _host_key_scan_process()
    create = AsyncMock(side_effect=[scan, capture, verify])

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=create,
    ):
        captured = await service.capture_vm_snapshot(
            job_id="terminal-thread",
            ssh_host="10.0.0.5",
            ssh_port=30022,
            source_type="pod",
            entity_type="threads",
            expected_host_key_fingerprint=fingerprint,
            strict_terminal=True,
        )

    assert captured is False
    service.upload_snapshot.assert_not_awaited()
    validator_command = create.await_args_list[2].args[-1]
    assert "zstd -t --" in validator_command
    assert ">/dev/null 2>&1" not in validator_command
    service._set_snapshot_context.assert_any_await(
        "terminal-thread",
        {
            "status": "capture_failed",
            "error": (
                "terminal snapshot archive validation failed: "
                "bash: line 1: zstd: command not found\n"
            ),
        },
        entity_type="threads",
    )


@pytest.mark.asyncio
async def test_strict_capture_uses_pipefail_keeps_git_and_stamps_digest() -> None:
    service = SnapshotService()
    service._available = True
    service.upload_snapshot = AsyncMock(return_value=True)
    capture = _capture_process(stdout=[b"complete-archive"], returncode=0)
    verify = MagicMock()
    verify.stderr.read = AsyncMock(side_effect=[b""])
    verify.wait = AsyncMock(return_value=0)
    verify.returncode = 0
    scan, fingerprint = _host_key_scan_process()
    create = AsyncMock(side_effect=[scan, capture, verify])

    with (
        patch.object(
            SnapshotService, "_collect_environment_info", AsyncMock(return_value={})
        ),
        patch(
            "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
            new=create,
        ),
    ):
        captured = await service.capture_vm_snapshot(
            job_id="terminal-thread",
            ssh_host="10.0.0.5",
            ssh_port=30022,
            source_type="pod",
            entity_type="threads",
            expected_host_key_fingerprint=fingerprint,
            strict_terminal=True,
        )

    assert captured is True
    ssh_command = create.await_args_list[1].args[-1]
    assert "bash -o pipefail" in ssh_command
    assert "--exclude=.git/objects" not in ssh_command
    assert "--exclude=*/repos/*" not in ssh_command
    assert "/usr/local/" not in ssh_command
    for pattern in CREDENTIAL_EXCLUDE_PATTERNS:
        assert f"{pattern} " in ssh_command
    manifest = service.upload_snapshot.await_args.kwargs["manifest"]
    assert manifest["strict_terminal"] is True
    assert manifest["captured_paths"] == ["/home/agent-host/"]
    assert (
        manifest["sha256_compressed"] == hashlib.sha256(b"complete-archive").hexdigest()
    )


def test_strict_archive_round_trip_preserves_git_objects_and_repo_state(
    tmp_path: Path,
) -> None:
    # Production roots live under /home; the pipeline intentionally excludes
    # /tmp, so create this fixture under the repository instead of pytest's
    # default /tmp base.
    with tempfile.TemporaryDirectory(
        prefix="snapshot-roundtrip-", dir=Path.home()
    ) as root:
        source = Path(root) / "workspace"
        git_object = source / ".git" / "objects" / "aa" / "object"
        repo_file = source / "repos" / "project" / "state.txt"
        git_object.parent.mkdir(parents=True)
        repo_file.parent.mkdir(parents=True)
        git_object.write_bytes(b"durable-git-object")
        repo_file.write_text("undo-authority", encoding="utf-8")
        archive = tmp_path / "workspace.tar.zst"

        command = _snapshot_tar_pipeline([str(source)], strict_terminal=True)
        with archive.open("wb") as output:
            completed = subprocess.run(
                ["bash", "-c", command],
                stdout=output,
                stderr=subprocess.PIPE,
                check=False,
            )
        assert completed.returncode == 0, completed.stderr.decode(errors="replace")

        restored = tmp_path / "restored"
        restored.mkdir()
        extracted = subprocess.run(
            [
                "bash",
                "-o",
                "pipefail",
                "-c",
                f"zstd -dc -- {archive} | tar -xf - -C {restored}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        assert extracted.returncode == 0, extracted.stderr.decode(errors="replace")
        restored_git = next(restored.rglob(".git/objects/aa/object"))
        restored_repo = next(restored.rglob("repos/project/state.txt"))
        assert restored_git.read_bytes() == b"durable-git-object"
        assert restored_repo.read_text(encoding="utf-8") == "undo-authority"


def test_strict_archive_omits_connector_credentials_and_keeps_user_ssh(
    tmp_path: Path,
) -> None:
    """The sole durable copy of an emptyDir home holds no connector credential.

    Env connector files, the managed ssh-agent namespace and legacy external
    repository keys are rewritten on every attach; the user's own ``~/.ssh``
    content must survive the terminal snapshot.
    """
    with tempfile.TemporaryDirectory(
        prefix="snapshot-credentials-", dir=Path.home()
    ) as root:
        home = Path(root) / "agent-host"
        fixture = {
            ".srw-credentials/0123abcd.sh": "export C0=env-secret\n",
            ".srw-credentials/0123abcd.json": "{}",
            ".ssh/srw-managed/config.d/srw-repo-x.conf": "Host srw-repo-x\n",
            ".ssh/srw-managed/agents/srw-repo-x.state": "pid=1\n",
            ".ssh/srw-managed/known_hosts": "gitea ssh-ed25519 AAAA\n",
            ".ssh/repo_github": "legacy-private-key\n",
            ".ssh/config": "Include ~/.ssh/srw-managed/config.d/*.conf\n",
            ".ssh/id_ed25519": "user-private-key\n",
            ".ssh/known_hosts": "example ssh-ed25519 AAAA\n",
            ".ssh/repository-notes": "user file with a similar name\n",
            "workspace/repos/project/state.txt": "undo-authority",
        }
        for relative, content in fixture.items():
            path = home / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        archive = tmp_path / "home.tar.zst"

        command = _snapshot_tar_pipeline([str(home)], strict_terminal=True)
        with archive.open("wb") as output:
            completed = subprocess.run(
                ["bash", "-c", command],
                stdout=output,
                stderr=subprocess.PIPE,
                check=False,
            )
        assert completed.returncode == 0, completed.stderr.decode(errors="replace")

    listing = subprocess.run(
        ["bash", "-o", "pipefail", "-c", f"zstd -dc -- {archive} | tar -tf -"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert not [m for m in listing if "/.srw-credentials" in m]
    assert not [m for m in listing if "/.ssh/srw-managed" in m]
    assert not [m for m in listing if "/.ssh/repo_" in m]
    for kept in (
        ".ssh/config",
        ".ssh/id_ed25519",
        ".ssh/known_hosts",
        ".ssh/repository-notes",
        "workspace/repos/project/state.txt",
    ):
        assert any(m.endswith(f"agent-host/{kept}") for m in listing), kept


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest",
    [
        None,
        {},
        {"strict_terminal": False},
        {"strict_terminal": True},
        {"strict_terminal": True, "sha256_compressed": "0" * 63},
        {"strict_terminal": True, "sha256_compressed": "G" * 64},
    ],
)
async def test_strict_download_rejects_manifest_downgrade(
    tmp_path: Path, manifest: object
) -> None:
    payload = b"strict-snapshot"
    destination = tmp_path / "snapshot.tar.zst"
    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    service._s3.get_object.return_value = {"Body": io.BytesIO(payload)}
    service.get_manifest = AsyncMock(return_value=manifest)

    restored = await service.download_snapshot(
        "thread-id",
        str(destination),
        entity_type="threads",
        require_strict_terminal=True,
    )

    assert restored is False


@pytest.mark.asyncio
async def test_strict_download_accepts_only_matching_digest(tmp_path: Path) -> None:
    payload = b"strict-snapshot"
    destination = tmp_path / "snapshot.tar.zst"
    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    service._s3.get_object.return_value = {"Body": io.BytesIO(payload)}
    service.get_manifest = AsyncMock(
        return_value={
            "strict_terminal": True,
            "sha256_compressed": hashlib.sha256(payload).hexdigest(),
        }
    )

    assert await service.download_snapshot(
        "thread-id",
        str(destination),
        entity_type="threads",
        require_strict_terminal=True,
    )


@pytest.mark.asyncio
async def test_cancelled_capture_kills_and_reaps_child() -> None:
    service = SnapshotService()
    service._available = True
    process = MagicMock()
    process.returncode = None
    process.stdout.read = AsyncMock(side_effect=asyncio.CancelledError)
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    process.kill = MagicMock()
    process.terminate = MagicMock()
    scan, fingerprint = _host_key_scan_process()

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=AsyncMock(side_effect=[scan, process]),
    ):
        with pytest.raises(asyncio.CancelledError):
            await service.capture_vm_snapshot(
                job_id="terminal-thread",
                ssh_host="10.0.0.5",
                ssh_port=30022,
                source_type="pod",
                entity_type="threads",
                expected_host_key_fingerprint=fingerprint,
                strict_terminal=True,
            )

    process.terminate.assert_called_once_with()
    process.kill.assert_not_called()
    assert process.wait.await_count >= 1


@pytest.mark.asyncio
async def test_strict_capture_timeout_kills_and_reaps_child(monkeypatch) -> None:
    async def _blocked_read(_size):
        await asyncio.sleep(3600)

    service = SnapshotService()
    service._available = True
    process = MagicMock()
    process.returncode = None
    process.stdout.read = AsyncMock(side_effect=_blocked_read)
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    process.kill = MagicMock()
    process.terminate = MagicMock()
    scan, fingerprint = _host_key_scan_process()
    monkeypatch.setenv("STATELESS_TERMINAL_SNAPSHOT_TIMEOUT_S", "0.01")

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=AsyncMock(side_effect=[scan, process]),
    ):
        captured = await service.capture_vm_snapshot(
            job_id="terminal-thread",
            ssh_host="10.0.0.5",
            ssh_port=30022,
            source_type="pod",
            entity_type="threads",
            expected_host_key_fingerprint=fingerprint,
            strict_terminal=True,
        )

    assert captured is False
    process.terminate.assert_called_once_with()
    process.kill.assert_not_called()
    assert process.wait.await_count >= 1


@pytest.mark.asyncio
async def test_regular_capture_has_timeout_and_reaps_child(monkeypatch) -> None:
    async def _blocked_read(_size):
        await asyncio.sleep(3600)

    service = SnapshotService()
    service._available = True
    process = MagicMock()
    process.returncode = None
    process.stdout.read = AsyncMock(side_effect=_blocked_read)
    process.stderr.read = AsyncMock(side_effect=[b""])
    process.wait = AsyncMock(return_value=0)
    process.terminate = MagicMock()
    process.kill = MagicMock()
    monkeypatch.setenv("SNAPSHOT_CAPTURE_TIMEOUT_S", "0.01")

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=process),
    ):
        captured = await service.capture_vm_snapshot(
            job_id="ordinary-job",
            ssh_host="10.0.0.5",
            ssh_port=22,
        )

    assert captured is False
    process.terminate.assert_called_once_with()
    process.kill.assert_not_called()
    assert process.wait.await_count >= 1


@pytest.mark.asyncio
async def test_capture_stderr_tail_is_bounded() -> None:
    stream = MagicMock()
    stream.read = AsyncMock(side_effect=[b"a" * 40000, b"b" * 40000, b"c" * 40000, b""])

    tail = await _read_stream_tail(stream, limit=65536)

    assert len(tail) == 65536
    assert tail == (b"b" * 25536) + (b"c" * 40000)


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
async def test_strict_capture_refuses_missing_or_malformed_pin_before_ssh(
    fingerprint,
) -> None:
    service = SnapshotService()
    service._available = True
    service.upload_snapshot = AsyncMock(return_value=True)
    create = AsyncMock(side_effect=AssertionError("strict pin precedes SSH"))

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=create,
    ):
        captured = await service.capture_vm_snapshot(
            job_id="terminal-thread",
            ssh_host="10.0.0.5",
            ssh_port=30022,
            source_type="pod",
            entity_type="threads",
            expected_host_key_fingerprint=fingerprint,
            strict_terminal=True,
        )

    assert captured is False
    create.assert_not_awaited()
    service.upload_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_available_upload_snapshot_promotes_history_and_canonical(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "env.tar.zst"
    archive.write_bytes(b"archive")
    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()

    uploaded = await service.upload_snapshot(
        job_id="job-id",
        tar_path=str(archive),
        manifest={"version": 1},
    )

    assert uploaded is True
    assert service._s3.upload_file.call_count == 1
    copied_keys = [call.args[2] for call in service._s3.copy.call_args_list]
    assert len(copied_keys) == 2
    assert any(
        "/history/" in key and key.endswith("/env.tar.zst") for key in copied_keys
    )
    assert "jobs/job-id/env.tar.zst" in copied_keys
    manifest_keys = [
        call.kwargs["Key"] for call in service._s3.put_object.call_args_list
    ]
    assert len(manifest_keys) == 2
    assert any(
        "/history/" in key and key.endswith("/manifest.json") for key in manifest_keys
    )
    assert "jobs/job-id/manifest.json" in manifest_keys


@pytest.mark.asyncio
async def test_cancelled_s3_upload_joins_blocking_writer_before_return(
    tmp_path: Path,
) -> None:
    archive = tmp_path / "env.tar.zst"
    archive.write_bytes(b"archive")
    entered = threading.Event()
    release = threading.Event()

    def _blocked_upload(*_args, **_kwargs):
        entered.set()
        assert release.wait(timeout=5)
        raise RuntimeError("writer failed after caller cancellation")

    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    service._s3.upload_file.side_effect = _blocked_upload

    uploading = asyncio.create_task(
        service.upload_snapshot(
            job_id="thread-id",
            tar_path=str(archive),
            manifest={"version": 1},
            entity_type="threads",
        )
    )
    deadline = asyncio.get_running_loop().time() + 2
    while not entered.is_set():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)

    uploading.cancel()
    await asyncio.sleep(0.02)
    assert not uploading.done()
    service._s3.put_object.assert_not_called()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await uploading

    service._s3.upload_file.assert_called_once()
    service._s3.put_object.assert_not_called()


@pytest.mark.asyncio
async def test_cancelled_snapshot_delete_joins_blocking_delete_before_return() -> None:
    entered = threading.Event()
    release = threading.Event()

    def _blocked_delete(*_args, **_kwargs):
        entered.set()
        assert release.wait(timeout=5)

    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    paginator = service._s3.get_paginator.return_value
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "threads/thread-id/env.tar.zst"}]}
    ]
    service._s3.delete_objects.side_effect = _blocked_delete
    service._set_snapshot_context = AsyncMock()

    deleting = asyncio.create_task(
        service.delete_snapshot("thread-id", entity_type="threads")
    )
    deadline = asyncio.get_running_loop().time() + 2
    while not entered.is_set():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)

    deleting.cancel()
    await asyncio.sleep(0.02)
    assert not deleting.done()
    service._set_snapshot_context.assert_not_awaited()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await deleting

    service._s3.delete_objects.assert_called_once()
    service._set_snapshot_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_snapshot_delete_joins_initial_prefix_list() -> None:
    entered = threading.Event()
    release = threading.Event()

    class _BlockedPages:
        def __iter__(self):
            entered.set()
            assert release.wait(timeout=5)
            return iter([])

    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    paginator = service._s3.get_paginator.return_value
    paginator.paginate.return_value = _BlockedPages()

    deleting = asyncio.create_task(
        service.delete_snapshot("thread-id", entity_type="threads")
    )
    deadline = asyncio.get_running_loop().time() + 2
    while not entered.is_set():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)

    deleting.cancel()
    await asyncio.sleep(0.02)
    assert not deleting.done()
    service._s3.delete_objects.assert_not_called()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await deleting

    service._s3.delete_objects.assert_not_called()


@pytest.mark.asyncio
async def test_snapshot_delete_rejects_partial_s3_batch_failure() -> None:
    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    paginator = service._s3.get_paginator.return_value
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "threads/thread-id/env.tar.zst"}]}
    ]
    service._s3.delete_objects.return_value = {
        "Deleted": [],
        "Errors": [{"Key": "threads/thread-id/env.tar.zst", "Code": "AccessDenied"}],
    }
    service._set_snapshot_context = AsyncMock()

    assert await service.delete_snapshot("thread-id", entity_type="threads") is False
    service._set_snapshot_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_snapshot_delete_requires_empty_prefix_after_batch() -> None:
    service = SnapshotService()
    service._available = True
    service._bucket = "snapshots"
    service._s3 = MagicMock()
    paginator = service._s3.get_paginator.return_value
    paginator.paginate.side_effect = [
        [{"Contents": [{"Key": "threads/thread-id/env.tar.zst"}]}],
        [{"Contents": [{"Key": "threads/thread-id/env.tar.zst"}]}],
    ]
    service._s3.delete_objects.return_value = {"Deleted": []}
    service._set_snapshot_context = AsyncMock()

    assert await service.delete_snapshot("thread-id", entity_type="threads") is False
    service._set_snapshot_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_recapture_keeps_an_available_snapshot() -> None:
    """A retried terminal teardown re-captures against a VM that is already
    going away. That failure must not downgrade the snapshot the first attempt
    uploaded — the artifact is still in S3 and restorable."""
    service = SnapshotService()
    service._available = True
    service._db = AsyncMock()
    service._db.get_job = AsyncMock(
        return_value={
            "id": "job-recapture",
            "context": {"snapshot": {"status": "available", "checksum_sha256": "abc"}},
        }
    )
    service._set_snapshot_context = AsyncMock()
    service.upload_snapshot = AsyncMock(return_value=True)
    failed = _capture_process(stdout=[], returncode=255)

    with patch(
        "orchestrator.services.snapshot_service.asyncio.create_subprocess_exec",
        new=AsyncMock(return_value=failed),
    ):
        captured = await service.capture_vm_snapshot(
            job_id="job-recapture",
            ssh_host="10.42.0.47",
            ssh_port=22,
            source_type="vm",
        )

    assert captured is False
    service.upload_snapshot.assert_not_awaited()
    for call in service._set_snapshot_context.await_args_list:
        assert call.args[1].get("status") != "capture_failed"
    # "capturing" is written before the SSH step; the failure must hand the
    # earlier snapshot back by restoring "available", not leave "capturing".
    last = service._set_snapshot_context.await_args_list[-1].args[1]
    assert last["status"] == "available"
    assert "rc=255" in last["last_capture_error"]
