"""Commit-then-effects on the cloud-sync side (stateless_turn_resilience.md 4a).

The turn-end push is split into a workspace STAGE (reads + temp files) and a
cloud TRANSMIT (conditional WebDAV writes under the writer fence, with durable
per-file progress), so the run_queue unit can complete while the transmit
continues off-slot and a successor resumes from ``push_progress`` instead of
re-uploading. The local-fs transport applies RFC 4918 preconditions exactly
as a WebDAV server would.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
import threading
from typing import Any

import pytest

from agent.api.lease_context import LeaseLostError
from agent.services.cloud_sync import base
from agent.services.cloud_sync.base import (
    CLOUD_PUSH_PARALLEL_UPLOADS,
    CloudSyncFenceLost,
    CloudSyncMarker,
)
from agent.services.cloud_sync.coordinator import (
    CloudSyncError,
    MountSync,
    WorkspaceSyncCoordinator,
)
from shared.cloud_sync_generations import (
    CloudSyncRequirement,
    encode_cloud_sync_baseline,
)
from tests._fs_backend import FilesystemTestBackend
from tests.cloud_sync._local_fs import LocalFsWorkspaceSync

THREAD = "11111111-1111-4111-8111-111111111111"
WORKSPACE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SCOPE = "a" * 64


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


async def _ok() -> None:
    return None


def _sync(tmp_path: Path) -> tuple[LocalFsWorkspaceSync, Path, Path]:
    ws = tmp_path / "ws"
    ws.mkdir()
    remote = tmp_path / "remote"
    sync = LocalFsWorkspaceSync(
        tmp_path / "unused-local",
        remote_root=remote,
        workspace_backend=FilesystemTestBackend(ws),
    )
    return sync, ws, remote


def _requirement(baseline: dict, *, generation: int = 7) -> CloudSyncRequirement:
    manifest, _encoded, sha = encode_cloud_sync_baseline(baseline)
    return CloudSyncRequirement(
        mount_id="mount-a",
        required_generation=generation,
        acknowledged_generation=0,
        required_lease_token=generation,
        workspace_generation=WORKSPACE,
        sync_scope_sha256=SCOPE,
        baseline_manifest=manifest,
        baseline_sha256=sha,
    )


def _coordinator(sync: LocalFsWorkspaceSync) -> WorkspaceSyncCoordinator:
    coordinator = WorkspaceSyncCoordinator(
        thread_id=THREAD, workspace_generation=WORKSPACE
    )
    coordinator.add(
        MountSync(
            mount_id="mount-a",
            target_path="/",
            sync=sync,
            sync_scope_sha256=SCOPE,
            generation_key="mount-a",
        )
    )
    return coordinator


# ---------------------------------------------------------------- stage / transmit


@pytest.mark.asyncio
@pytest.mark.parametrize("bulk_fails", [False, True])
async def test_stage_uses_one_bulk_listing_and_falls_back_strictly(
    tmp_path, bulk_fails
):
    sync, ws, _remote = _sync(tmp_path)
    (ws / "nested").mkdir()
    (ws / "nested" / "a.txt").write_bytes(b"alpha")
    sync._mount_subdir = "nested"

    def bulk(path):
        assert path == "nested"
        if bulk_fails:
            raise OSError("bulk listing failed")
        return [("nested/a.txt", 5), ("nested/.srw/ignored.txt", 4)]

    def list_dir(path):
        raise OSError("strict listing failed")

    sync._backend.list_files_with_sizes = bulk
    sync._backend.list_dir = list_dir
    if bulk_fails:
        with pytest.raises(OSError, match="strict listing failed"):
            await sync.stage_generation_delta({})
    else:
        staged = await sync.stage_generation_delta({})
        try:
            assert [u.path for u in staged.uploads] == ["a.txt"]
            assert Path(staged.uploads[0].tmp_path).read_bytes() == b"alpha"
        finally:
            staged.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_stage_bounds_reads_and_drains_them_before_cleanup(
    tmp_path, monkeypatch, outcome
):
    sync, ws, _remote = _sync(tmp_path)
    for index in range(12):
        (ws / f"{index:02}.txt").write_bytes(str(index).encode())
    backend_read = sync._backend.read_file
    active = 0
    peak = 0
    calls = 0
    lock = threading.Lock()
    release = threading.Event()
    started = asyncio.Event()
    loop = asyncio.get_running_loop()
    temp = tmp_path / "staged"
    temp.mkdir()
    monkeypatch.setattr("tempfile.tempdir", str(temp))

    def read(path, binary):
        nonlocal active, peak, calls
        with lock:
            active += 1
            calls += 1
            peak = max(peak, active)
            if active == 4:
                loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(5), "staging did not overlap four reads"
            if outcome == "failure" and path.endswith("00.txt"):
                raise OSError("workspace read failed")
            return backend_read(path, binary)
        finally:
            with lock:
                active -= 1

    sync._backend.read_file = read
    task = asyncio.create_task(sync.stage_generation_delta({}))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        if outcome == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done(), "cancellation must wait for active backend reads"
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done(), "repeated cancellation must also drain reads"
        release.set()
        if outcome == "success":
            staged = await task
            assert [u.path for u in staged.uploads] == [
                f"{i:02}.txt" for i in range(12)
            ]
            assert all(
                Path(u.tmp_path).read_bytes() == str(i).encode()
                for i, u in enumerate(staged.uploads)
            )
            staged.cleanup()
        elif outcome == "failure":
            with pytest.raises(OSError, match="workspace read failed"):
                await task
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert calls == 4
        assert peak == 4 and active == 0
        assert list(temp.iterdir()) == []
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_stage_then_transmit_uses_preconditions_and_cleans_temp_files(
    tmp_path: Path,
):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    (ws / "sub").mkdir()
    (ws / "sub" / "b.txt").write_bytes(b"bravo")

    staged = await sync.stage_generation_delta({})
    assert [u.path for u in staged.uploads] == ["a.txt", "sub/b.txt"]
    assert all(u.if_none_match and u.if_match is None for u in staged.uploads)
    assert staged.planned == 2 and staged.skipped_by_progress == 0
    tmp_paths = [u.tmp_path for u in staged.uploads]
    assert all(os.path.exists(p) for p in tmp_paths)

    landed: list[tuple[str, dict[str, Any]]] = []

    async def progress(path: str, entry: dict[str, Any]) -> None:
        landed.append((path, entry))

    commit = await sync.transmit_generation_delta(
        staged, before_write=_ok, progress_cb=progress
    )
    assert (remote / "a.txt").read_bytes() == b"alpha"
    assert (remote / "sub" / "b.txt").read_bytes() == b"bravo"
    assert not any(os.path.exists(p) for p in tmp_paths)
    # Parallel uploads land in any order; commit.paths keeps the staged one.
    assert sorted(sync.conditional_writes) == [
        ("a.txt", None, True),
        ("sub/b.txt", None, True),
    ]
    assert commit.paths == ["a.txt", "sub/b.txt"]
    # the manifest now carries the server's etag for every landed path
    assert commit.manifest["a.txt"] == {
        "sha256": _sha(b"alpha"),
        "remote_etag": _sha(b"alpha"),
    }
    assert sorted(p for p, _ in landed) == ["a.txt", "sub/b.txt"]
    assert dict(landed)["a.txt"] == {
        "sha256": _sha(b"alpha"),
        "size": 5,
        "remote_etag": _sha(b"alpha"),
        "state": "uploaded",
    }


@pytest.mark.asyncio
async def test_changed_baseline_file_is_written_with_if_match(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (remote / "a.txt").write_bytes(b"old")
    (ws / "a.txt").write_bytes(b"new")
    baseline = {"a.txt": {"sha256": _sha(b"old"), "remote_etag": _sha(b"old")}}
    staged = await sync.stage_generation_delta(baseline)
    assert [(u.path, u.if_match, u.if_none_match) for u in staged.uploads] == [
        ("a.txt", _sha(b"old"), False)
    ]
    await sync.transmit_generation_delta(staged, before_write=_ok)
    assert (remote / "a.txt").read_bytes() == b"new"


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
async def test_delayed_marker_cannot_replace_successor_commit(tmp_path, existing):
    old, _workspace, remote = _sync(tmp_path)
    successor = LocalFsWorkspaceSync(tmp_path / "successor", remote_root=remote)

    def marker(generation):
        manifest, _encoded, digest = encode_cloud_sync_baseline(
            {"a.txt": {"sha256": _sha(str(generation).encode()), "remote_etag": "e"}}
        )
        return CloudSyncMarker(
            thread_id=THREAD,
            mount_id="mount-a",
            generation=generation,
            lease_token=generation,
            workspace_generation=WORKSPACE,
            sync_scope_sha256=SCOPE,
            committed_manifest=manifest,
            committed_manifest_sha256=digest,
        )

    if existing:
        await old.write_sync_generation_marker(marker(1), before_write=_ok)
    ready = asyncio.Event()
    release = asyncio.Event()
    upload = old._upload_file

    async def delayed_upload(path, local_path, *, before_write=None, **kwargs):
        if before_write is not None:
            await before_write()
        ready.set()
        await release.wait()  # the request passed its last DB check
        return await upload(path, local_path, **kwargs)

    old._upload_file = delayed_upload
    writer = asyncio.create_task(
        old.write_sync_generation_marker(marker(2), before_write=_ok)
    )
    try:
        await asyncio.wait_for(ready.wait(), timeout=2)
        await successor.write_sync_generation_marker(marker(3), before_write=_ok)
        release.set()
        with pytest.raises(CloudSyncFenceLost):
            await writer
        persisted = await successor.read_sync_generation_marker(
            thread_id=THREAD, sync_scope_sha256=SCOPE
        )
        assert persisted == marker(3)
    finally:
        release.set()
        await asyncio.gather(writer, return_exceptions=True)


@pytest.mark.asyncio
async def test_marker_cleanup_does_not_close_reused_transport_descriptor(tmp_path):
    sync, ws, _remote = _sync(tmp_path)
    sentinel = ws / "transport.txt"
    sentinel.write_bytes(b"still open")
    opened = []

    async def upload(*args, **kwargs):
        opened.append(os.open(sentinel, os.O_RDONLY))

    sync._upload_file = upload
    marker = CloudSyncMarker(
        thread_id=THREAD,
        mount_id="mount-a",
        generation=1,
        lease_token=1,
        workspace_generation=WORKSPACE,
        sync_scope_sha256=SCOPE,
    )
    try:
        await sync.write_sync_generation_marker(marker, before_write=_ok)
        assert os.read(opened[0], 20) == b"still open"
    finally:
        for fd in opened:
            try:
                os.close(fd)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_412_with_a_live_fence_retries_once_with_the_fresh_etag(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (remote / "a.txt").write_bytes(b"user-edit")  # moved under us
    (ws / "a.txt").write_bytes(b"agent")
    baseline = {"a.txt": {"sha256": _sha(b"old"), "remote_etag": "stale-etag"}}
    staged = await sync.stage_generation_delta(baseline)
    fence_checks = 0

    async def fence() -> None:
        nonlocal fence_checks
        fence_checks += 1

    await sync.transmit_generation_delta(staged, before_write=fence)
    assert (remote / "a.txt").read_bytes() == b"agent"
    # first attempt carried the stale precondition, the retry the fresh one
    assert [w[1] for w in sync.conditional_writes] == [_sha(b"user-edit")]
    assert fence_checks >= 3  # transmit start, per-write, the post-412 recheck


@pytest.mark.asyncio
async def test_412_with_a_lost_fence_stops_and_cleans_up(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (remote / "a.txt").write_bytes(b"successor-bytes")
    (ws / "a.txt").write_bytes(b"stale-owner-bytes")
    baseline = {"a.txt": {"sha256": _sha(b"old"), "remote_etag": "stale-etag"}}
    staged = await sync.stage_generation_delta(baseline)
    tmp_paths = [u.tmp_path for u in staged.uploads]
    checks = 0

    class Lost(RuntimeError):
        pass

    async def fence() -> None:
        # The DB says we were adopted away the moment the write bounced.
        nonlocal checks
        checks += 1
        if checks >= 3:
            raise Lost("push-owner fence")

    with pytest.raises(Lost):
        await sync.transmit_generation_delta(staged, before_write=fence)
    assert (remote / "a.txt").read_bytes() == b"successor-bytes"  # never clobbered
    assert not any(os.path.exists(p) for p in tmp_paths)


@pytest.mark.asyncio
async def test_progress_resumes_uploads_and_skips_recorded_deletes(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    (ws / "b.txt").write_bytes(b"bravo")
    (remote / "a.txt").write_bytes(b"alpha")  # the predecessor landed a.txt
    baseline = {"gone.txt": {"sha256": _sha(b"x"), "remote_etag": "g"}}
    progress = {
        "planned": 3,
        "files": {
            "a.txt": {
                "sha256": _sha(b"alpha"),
                "size": 5,
                "remote_etag": _sha(b"alpha"),
                "state": "uploaded",
            },
            "gone.txt": {"state": "deleted"},
        },
    }
    staged = await sync.stage_generation_delta(baseline, progress=progress)
    assert [u.path for u in staged.uploads] == ["b.txt"]
    assert staged.deletes == []
    assert staged.skipped_by_progress == 2 and staged.planned == 3
    commit = await sync.transmit_generation_delta(staged, before_write=_ok)
    assert commit.paths == ["b.txt"]
    assert commit.manifest["a.txt"]["remote_etag"] == _sha(b"alpha")
    assert "gone.txt" not in commit.manifest


@pytest.mark.asyncio
async def test_deletes_carry_if_match_and_report_progress(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (remote / "old.txt").write_bytes(b"old")
    baseline = {"old.txt": {"sha256": _sha(b"old"), "remote_etag": _sha(b"old")}}
    staged = await sync.stage_generation_delta(baseline)
    assert [(d.path, d.if_match) for d in staged.deletes] == [("old.txt", _sha(b"old"))]
    landed: list[tuple[str, dict[str, Any]]] = []

    async def progress(path: str, entry: dict[str, Any]) -> None:
        landed.append((path, entry))

    commit = await sync.transmit_generation_delta(
        staged, before_write=_ok, progress_cb=progress
    )
    assert not (remote / "old.txt").exists()
    assert landed == [
        ("old.txt", {"sha256": "", "size": 0, "remote_etag": "", "state": "deleted"})
    ]
    assert commit.manifest == {}


# ------------------------------------------------------- parallel transmit


PATHS = [f"{index:02}.txt" for index in range(10)]
OLD = {"old.txt": {"sha256": _sha(b"old"), "remote_etag": _sha(b"old")}}


async def _ten_uploads_and_a_delete(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    for path in PATHS:
        (ws / path).write_bytes(path.encode())
    (remote / "old.txt").write_bytes(b"old")
    staged = await sync.stage_generation_delta(OLD)
    assert [u.path for u in staged.uploads] == PATHS
    assert [d.path for d in staged.deletes] == ["old.txt"]
    return sync, remote, staged


class _InFlight:
    """Holds every upload after its last DB check, as if on the wire.

    ``started`` lists the uploads that passed their fence; ``full`` is set
    once a whole batch is in flight; ``release`` lets them all answer.
    """

    def __init__(self, sync: LocalFsWorkspaceSync, answers: dict | None = None):
        self.started: list[str] = []
        self.full = asyncio.Event()
        self.release = asyncio.Event()
        self._answers = answers or {}
        self._upload = sync._upload_file
        sync._upload_file = self.upload

    async def upload(self, path, local_path, *, before_write=None, **kwargs):
        await before_write()  # the request's last DB check passed ...
        self.started.append(path)
        if len(self.started) == CLOUD_PUSH_PARALLEL_UPLOADS:
            self.full.set()
        await self.release.wait()  # ... and it is on the wire
        if path in self._answers:
            raise self._answers[path]
        return await self._upload(path, local_path, **kwargs)


@pytest.mark.asyncio
async def test_uploads_run_k_at_a_time_and_all_land_before_any_delete(tmp_path):
    sync, remote, staged = await _ten_uploads_and_a_delete(tmp_path)
    events: list[tuple[str, str]] = []
    active = peak = 0
    upload, delete = sync._upload_file, sync._delete_remote_file

    async def slow_upload(path, local_path, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            # Later paths answer sooner, so landing order is not staged order.
            await asyncio.sleep(0.005 * (len(PATHS) - PATHS.index(path)))
            return await upload(path, local_path, **kwargs)
        finally:
            active -= 1
            events.append(("put", path))

    async def recorded_delete(path, **kwargs):
        events.append(("delete", path))
        await delete(path, **kwargs)

    sync._upload_file = slow_upload
    sync._delete_remote_file = recorded_delete
    landed: list[str] = []

    async def progress(path: str, entry: dict[str, Any]) -> None:
        landed.append(path)

    commit = await sync.transmit_generation_delta(
        staged, before_write=_ok, progress_cb=progress
    )
    assert peak == CLOUD_PUSH_PARALLEL_UPLOADS == 4
    assert len(events) == 11 and events[-1] == ("delete", "old.txt")
    assert landed[:-1] != PATHS and sorted(landed) == sorted(PATHS + ["old.txt"])
    assert commit.paths == PATHS + ["old.txt"]
    assert sorted(commit.manifest) == PATHS
    assert sorted(p.name for p in remote.iterdir()) == PATHS


@pytest.mark.asyncio
@pytest.mark.parametrize("fenced", [LeaseLostError, CloudSyncFenceLost])
async def test_fence_loss_mid_batch_lets_in_flight_writes_land_and_starts_no_more(
    tmp_path, fenced
):
    sync, remote, staged = await _ten_uploads_and_a_delete(tmp_path)
    tmp_paths = [u.tmp_path for u in staged.uploads]
    lost = False

    async def fence() -> None:
        if lost:
            raise fenced("adopted by a successor")

    wire = _InFlight(sync)
    landed: list[str] = []

    async def progress(path: str, entry: dict[str, Any]) -> None:
        landed.append(path)

    task = asyncio.create_task(
        sync.transmit_generation_delta(staged, before_write=fence, progress_cb=progress)
    )
    try:
        await asyncio.wait_for(wire.full.wait(), timeout=2)
        lost = True
        wire.release.set()
        with pytest.raises(fenced):
            await task
    finally:
        wire.release.set()
        await asyncio.gather(task, return_exceptions=True)
    in_flight = PATHS[:CLOUD_PUSH_PARALLEL_UPLOADS]
    assert wire.started == in_flight  # nothing new after the flip
    assert sorted(landed) == in_flight  # landed writes are still recorded
    # ... and the delete was never issued
    assert sorted(p.name for p in remote.iterdir()) == in_flight + ["old.txt"]
    assert not any(os.path.exists(p) for p in tmp_paths)


@pytest.mark.asyncio
async def test_a_fence_loss_is_raised_over_an_earlier_ordinary_error(tmp_path):
    sync, remote, staged = await _ten_uploads_and_a_delete(tmp_path)
    lost = False

    async def fence() -> None:
        if lost:
            raise LeaseLostError("push-owner fence")

    # 00 fails on the wire first; 01 gets a 412 and its recheck finds the
    # fence gone. The stale writer must report the fence, not the reset.
    wire = _InFlight(
        sync,
        {
            "00.txt": OSError("connection reset"),
            "01.txt": CloudSyncFenceLost("412"),
        },
    )
    task = asyncio.create_task(
        sync.transmit_generation_delta(staged, before_write=fence)
    )
    try:
        await asyncio.wait_for(wire.full.wait(), timeout=2)
        lost = True
        wire.release.set()
        with pytest.raises(LeaseLostError):
            await task
    finally:
        wire.release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert sorted(p.name for p in remote.iterdir()) == ["02.txt", "03.txt", "old.txt"]


@pytest.mark.asyncio
async def test_cancelled_transmit_waits_for_the_uploads_in_flight(tmp_path):
    sync, remote, staged = await _ten_uploads_and_a_delete(tmp_path)
    tmp_paths = [u.tmp_path for u in staged.uploads]
    wire = _InFlight(sync)
    landed: list[str] = []

    async def progress(path: str, entry: dict[str, Any]) -> None:
        landed.append(path)

    task = asyncio.create_task(
        sync.transmit_generation_delta(staged, before_write=_ok, progress_cb=progress)
    )
    try:
        await asyncio.wait_for(wire.full.wait(), timeout=2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "cancellation must wait for writes in flight"
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "repeated cancellation must also wait"
        assert all(os.path.exists(p) for p in tmp_paths)  # still being read
        wire.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        wire.release.set()
        await asyncio.gather(task, return_exceptions=True)
    in_flight = PATHS[:CLOUD_PUSH_PARALLEL_UPLOADS]
    assert wire.started == in_flight
    assert sorted(landed) == in_flight
    assert sorted(p.name for p in remote.iterdir()) == in_flight + ["old.txt"]
    assert not any(os.path.exists(p) for p in tmp_paths)


class _Status(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(f"HTTP {code}")
        self.code = code


class ResourceLocked(Exception):
    """webdav3's 423: no ``code`` attribute, recognised by name."""


async def _flaky_single_upload(tmp_path, monkeypatch, errors):
    monkeypatch.setattr(base, "_BUSY_WRITE_RETRY_DELAYS", (0.0, 0.0, 0.0))
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    staged = await sync.stage_generation_delta({})
    upload = sync._upload_file
    attempts = 0
    fence_checks = 0

    async def flaky(path, local_path, *, before_write=None, **kwargs):
        nonlocal attempts
        attempts += 1
        await before_write()
        if errors:
            raise errors.pop(0)
        return await upload(path, local_path, **kwargs)

    async def fence() -> None:
        nonlocal fence_checks
        fence_checks += 1

    sync._upload_file = flaky
    try:
        commit = await sync.transmit_generation_delta(staged, before_write=fence)
    except Exception as exc:
        commit = exc
    return commit, attempts, fence_checks, remote


@pytest.mark.asyncio
@pytest.mark.parametrize("busy", [503, 423])
async def test_busy_answers_are_retried_and_every_attempt_is_fenced(
    tmp_path, monkeypatch, busy
):
    error = _Status(503) if busy == 503 else ResourceLocked("/a.txt")
    commit, attempts, fence_checks, remote = await _flaky_single_upload(
        tmp_path, monkeypatch, [error, error]
    )
    assert commit.paths == ["a.txt"] and attempts == 3
    # transmit start, the upload's own check, then one per attempt
    assert fence_checks == 2 + 3
    assert (remote / "a.txt").read_bytes() == b"alpha"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error, attempts", [(_Status(503), 4), (_Status(500), 1), (_Status(507), 1)]
)
async def test_busy_retries_are_bounded_and_other_errors_fail_at_once(
    tmp_path, monkeypatch, error, attempts
):
    outcome, tried, _checks, remote = await _flaky_single_upload(
        tmp_path, monkeypatch, [error] * 10
    )
    assert outcome is error and tried == attempts
    assert not (remote / "a.txt").exists()


# ---------------------------------------------------------------- coordinator


@pytest.mark.asyncio
async def test_stage_needs_no_cloud_io_and_transmit_needs_no_workspace(tmp_path):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    coordinator = _coordinator(sync)
    read_marker = sync.read_sync_generation_marker

    async def unavailable(**kwargs):
        raise AssertionError("cloud marker read must happen off-slot")

    sync.read_sync_generation_marker = unavailable
    staged = await coordinator.stage_generation({"mount-a": _requirement({})})
    sync.read_sync_generation_marker = read_marker
    sync._backend = None  # A detached workspace is unavailable to transmit.

    async def acknowledge(*args):
        pass

    await coordinator.transmit_generation(
        staged, before_write=_ok, acknowledge=acknowledge
    )
    assert (remote / "a.txt").read_bytes() == b"alpha"


@pytest.mark.asyncio
@pytest.mark.parametrize("marker_failure", [False, True])
async def test_off_slot_marker_check_cleans_staging_and_preserves_later_cloud_edits(
    tmp_path, marker_failure
):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    coordinator = _coordinator(sync)
    requirement = _requirement({})

    async def acknowledge(*args):
        pass

    await coordinator.push_generation(
        {"mount-a": requirement}, before_write=_ok, acknowledge=acknowledge
    )
    (ws / "a.txt").write_bytes(b"stale retry")
    (remote / "a.txt").write_bytes(b"later cloud edit")
    staged = await coordinator.stage_generation({"mount-a": requirement})
    paths = [u.tmp_path for u in staged[0].staged.uploads]
    assert paths
    if marker_failure:

        async def failed(**kwargs):
            raise OSError("marker unavailable")

        sync.read_sync_generation_marker = failed
        with pytest.raises(CloudSyncError):
            await coordinator.transmit_generation(
                staged, before_write=_ok, acknowledge=acknowledge
            )
    else:
        assert await coordinator.transmit_generation(
            staged, before_write=_ok, acknowledge=acknowledge
        ) == {"mount-a": []}
    assert (remote / "a.txt").read_bytes() == b"later cloud edit"
    assert not any(os.path.exists(path) for path in paths)


@pytest.mark.asyncio
async def test_coordinator_stage_transmit_writes_marker_acks_and_reports(
    tmp_path: Path,
):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    coordinator = _coordinator(sync)
    requirement = _requirement({})
    acked: list[str] = []
    planned: list[tuple[str, int]] = []
    landed: list[tuple[str, str]] = []

    async def acknowledge(mount_id: str, req: Any) -> None:
        acked.append(mount_id)

    async def on_planned(mount_id: str, n: int) -> None:
        planned.append((mount_id, n))

    async def on_progress(mount_id: str, path: str, entry: dict[str, Any]) -> None:
        landed.append((mount_id, path))

    staged = await coordinator.stage_generation({"mount-a": requirement})
    assert staged[0].staged is not None and staged[0].staged.planned == 1
    out = await coordinator.transmit_generation(
        staged,
        before_write=_ok,
        acknowledge=acknowledge,
        progress=on_progress,
        planned=on_planned,
    )
    assert out == {"mount-a": ["a.txt"]}
    assert acked == ["mount-a"] and planned == [("mount-a", 1)]
    assert landed == [("mount-a", "a.txt")]
    marker = await sync.read_sync_generation_marker(
        thread_id=THREAD, sync_scope_sha256=SCOPE
    )
    assert marker is not None and marker.generation == 7
    assert marker.committed_manifest["a.txt"]["remote_etag"] == _sha(b"alpha")
    # push_generation is the same two halves in one call
    (ws / "b.txt").write_bytes(b"bravo")
    again = await coordinator.push_generation(
        {"mount-a": _requirement(marker.committed_manifest, generation=8)},
        before_write=_ok,
        acknowledge=acknowledge,
    )
    assert again == {"mount-a": ["b.txt"]}


@pytest.mark.asyncio
async def test_reconcile_adopts_once_and_resumes_from_the_predecessor(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    (ws / "b.txt").write_bytes(b"bravo")
    (remote / "a.txt").write_bytes(b"alpha")  # predecessor landed this, then died
    coordinator = _coordinator(sync)
    requirement = _requirement({})  # pending: acknowledged 0 < required 7
    adopt_calls = 0
    acked: list[str] = []

    async def adopt() -> dict[str, Any]:
        nonlocal adopt_calls
        adopt_calls += 1
        return {
            "mount-a": {
                "planned": 2,
                "files": {
                    "a.txt": {
                        "sha256": _sha(b"alpha"),
                        "size": 5,
                        "remote_etag": _sha(b"alpha"),
                        "state": "uploaded",
                    }
                },
            }
        }

    async def acknowledge(mount_id: str, req: Any) -> None:
        acked.append(mount_id)

    out = await coordinator.reconcile_before_pull(
        {"mount-a": requirement}, before_write=_ok, acknowledge=acknowledge, adopt=adopt
    )
    assert adopt_calls == 1
    assert out == {"mount-a": ["b.txt"]}  # a.txt resumed, not re-uploaded
    assert [w[0] for w in sync.conditional_writes if not w[0].startswith(".srw/")] == [
        "b.txt"
    ]
    assert acked == ["mount-a"]
    marker = await sync.read_sync_generation_marker(
        thread_id=THREAD, sync_scope_sha256=SCOPE
    )
    assert marker is not None and set(marker.committed_manifest) == {"a.txt", "b.txt"}


@pytest.mark.asyncio
async def test_repeated_recovery_retains_newly_landed_progress(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    (ws / "b.txt").write_bytes(b"bravo")
    coordinator = _coordinator(sync)
    requirement = _requirement({})
    durable = {"mount-a": {"planned": 2, "files": {}}}
    acknowledgements = []

    async def adopt():
        return durable

    async def record(mount_id, path, entry):
        durable[mount_id]["files"][path] = entry
        raise RuntimeError("recovery pod died after checkpoint")

    async def acknowledge(mount_id, _requirement):
        acknowledgements.append(mount_id)

    with pytest.raises(CloudSyncError):
        await coordinator.reconcile_before_pull(
            {"mount-a": requirement},
            before_write=_ok,
            acknowledge=acknowledge,
            adopt=adopt,
            progress=record,
        )
    assert not acknowledgements
    assert list(durable["mount-a"]["files"]) == ["a.txt"]
    await coordinator.reconcile_before_pull(
        {"mount-a": requirement}, before_write=_ok, acknowledge=acknowledge, adopt=adopt
    )
    assert acknowledgements == ["mount-a"]
    assert [
        path for path, *_ in sync.conditional_writes if not path.startswith(".srw/")
    ] == ["a.txt", "b.txt"]
    assert (remote / "b.txt").read_bytes() == b"bravo"


@pytest.mark.asyncio
async def test_reconcile_does_not_adopt_when_nothing_is_pending(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    coordinator = _coordinator(sync)
    manifest, _e, sha = encode_cloud_sync_baseline({})
    done = CloudSyncRequirement(
        mount_id="mount-a",
        required_generation=7,
        acknowledged_generation=7,
        required_lease_token=7,
        workspace_generation=WORKSPACE,
        sync_scope_sha256=SCOPE,
        baseline_manifest=manifest,
        baseline_sha256=sha,
    )
    await sync.write_sync_generation_marker(
        CloudSyncMarker(
            thread_id=THREAD,
            mount_id="mount-a",
            generation=7,
            lease_token=7,
            workspace_generation=WORKSPACE,
            sync_scope_sha256=SCOPE,
            baseline_sha256=sha,
        )
    )
    adopt_calls = 0

    async def adopt() -> dict[str, Any]:
        nonlocal adopt_calls
        adopt_calls += 1
        return {}

    async def acknowledge(mount_id: str, req: Any) -> None:
        raise AssertionError("nothing to acknowledge")

    await coordinator.reconcile_before_pull(
        {"mount-a": done}, before_write=_ok, acknowledge=acknowledge, adopt=adopt
    )
    assert adopt_calls == 0


@pytest.mark.asyncio
async def test_stage_failure_cleans_every_mount(tmp_path: Path):
    sync, ws, remote = _sync(tmp_path)
    (ws / "a.txt").write_bytes(b"alpha")
    coordinator = _coordinator(sync)
    broken = _requirement({})
    original = sync.stage_generation_delta

    async def boom(*args: Any, **kwargs: Any):
        staged = await original(*args, **kwargs)
        staged.cleanup()
        raise RuntimeError("workspace vanished")

    sync.stage_generation_delta = boom  # type: ignore[method-assign]
    with pytest.raises(CloudSyncError):
        await coordinator.stage_generation({"mount-a": broken})
