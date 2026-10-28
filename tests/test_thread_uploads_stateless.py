"""Exact workspace-authority tests for stateless sandbox uploads."""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
import asyncssh

from orchestrator.services.canvas_ssh import CanvasSSHError
from orchestrator.services.thread_uploads import (
    ThreadUploadError,
    _SshTarget,
    _VirtualTarget,
    _joined_async_call,
    _virtual_purge_prefix,
    delete_file_from_attested_stateless_workspace,
    delete_file_from_thread_workspace,
    purge_attested_stateless_virtual_workspace,
    resolve_thread_upload_destination,
    upload_files_to_attested_stateless_workspace,
    upload_files_to_thread_workspace,
)


THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
GENERATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
RUNTIME = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
FINGERPRINT = "SHA256:provisioner-attested"
UPLOAD_PATH = "/home/agent-host/workspace/uploads/notes.txt"


def _sandbox_thread() -> dict:
    return {
        "id": THREAD_ID,
        "execution_lane": "stateless",
        "status": "active",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.0.25",
                "port": 30022,
                "pod_name": "ws-thread-aaaaaaaaaaaa",
                "namespace": "agent-workspaces",
                "_canvas_workspace_generation": GENERATION,
                "_runtime_incarnation": RUNTIME,
            },
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
        },
    }


class _AsyncFile:
    def __init__(self, sink: dict[str, bytes], path: str):
        self._sink = sink
        self._path = path
        self._data = b""

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        del exc, traceback
        if exc_type is None:
            self._sink[self._path] = self._data
        return False

    async def write(self, data: bytes) -> None:
        self._data += data


class _AsyncSFTP:
    def __init__(self):
        self.dirs: set[str] = {""}
        self.files: dict[str, bytes] = {}
        self.types: dict[str, int] = {}
        self.write_attempts = 0

    async def stat(self, path: str):
        if path in self.dirs or path in self.files:
            return object()
        raise FileNotFoundError(path)

    async def lstat(self, path: str):
        if path in self.dirs:
            return SimpleNamespace(
                type=self.types.get(path, asyncssh.FILEXFER_TYPE_DIRECTORY),
                permissions=None,
            )
        if path in self.files:
            return SimpleNamespace(
                type=self.types.get(path, asyncssh.FILEXFER_TYPE_REGULAR),
                permissions=None,
            )
        raise FileNotFoundError(path)

    async def mkdir(self, path: str) -> None:
        self.dirs.add(path)

    async def listdir(self, path: str) -> list[str]:
        prefix = path.rstrip("/") + "/"
        children: set[str] = set()
        for existing in (*self.dirs, *self.files):
            if existing.startswith(prefix) and existing != path:
                remainder = existing[len(prefix) :]
                if remainder:
                    children.add(remainder.split("/", 1)[0])
        return sorted(children)

    def open(self, path: str, mode: str) -> _AsyncFile:
        assert mode == "xb"
        if path in self.files:
            raise FileExistsError(path)
        self.write_attempts += 1
        return _AsyncFile(self.files, path)

    async def remove(self, path: str) -> None:
        if path not in self.files:
            raise FileNotFoundError(path)
        del self.files[path]

    async def rmdir(self, path: str) -> None:
        prefix = path.rstrip("/") + "/"
        if any(item.startswith(prefix) for item in (*self.dirs, *self.files)):
            raise OSError("directory not empty")
        self.dirs.remove(path)


class _PinnedPool:
    def __init__(self, sftp: _AsyncSFTP):
        self.sftp = sftp
        self.checkouts: list[dict] = []

    @asynccontextmanager
    async def checkout(self, **kwargs):
        self.checkouts.append(kwargs)
        yield self.sftp


class _RejectingPinnedPool(_PinnedPool):
    @asynccontextmanager
    async def checkout(self, **kwargs):
        self.checkouts.append(kwargs)
        raise CanvasSSHError(
            503,
            "workspace_unavailable",
            "server host key did not match the pinned fingerprint",
        )
        yield self.sftp  # pragma: no cover - makes this an async context manager


@pytest.fixture
def attested_upload(monkeypatch):
    from orchestrator.services import thread_uploads

    monkeypatch.setattr(thread_uploads, "resolve_ssh_key_path", lambda: "/ssh/key")
    thread = _sandbox_thread()
    target = resolve_thread_upload_destination(thread)
    assert isinstance(target, _SshTarget)
    sftp = _AsyncSFTP()
    pool = _PinnedPool(sftp)
    monkeypatch.setattr(thread_uploads, "_ATTESTED_SFTP_POOL", pool)
    return SimpleNamespace(thread=thread, target=target, sftp=sftp, pool=pool)


async def _upload(env, probe, **overrides):
    arguments = {
        "expected_workspace_generation": GENERATION,
        "expected_runtime_incarnation": RUNTIME,
        "expected_host_key_fingerprint": FINGERPRINT,
        "authority_probe": probe,
    }
    arguments.update(overrides)
    return await upload_files_to_attested_stateless_workspace(
        env.thread,
        [("notes.txt", b"hello", "text/plain")],
        destination=env.target,
        **arguments,
    )


@pytest.mark.asyncio
async def test_attested_upload_pins_generation_runtime_and_host_key(attested_upload):
    probe = AsyncMock(return_value="exact_live")

    result = await _upload(attested_upload, probe)

    assert result[0].path == "uploads/notes.txt"
    assert attested_upload.sftp.files == {UPLOAD_PATH: b"hello"}
    assert probe.await_count == 4
    checkout = attested_upload.pool.checkouts[0]
    assert checkout["thread_id"] == f"{THREAD_ID}:{RUNTIME}"
    assert checkout["generation"] == UUID(GENERATION)
    assert checkout["fingerprint"] == FINGERPRINT
    assert checkout["host"] == "10.42.0.25"
    assert checkout["port"] == 30022


@pytest.mark.asyncio
async def test_attested_delete_pins_generation_runtime_and_host_key(attested_upload):
    uploads_dir = "/home/agent-host/workspace/uploads"
    attested_upload.sftp.dirs.update(
        {
            "/home",
            "/home/agent-host",
            "/home/agent-host/workspace",
            uploads_dir,
        }
    )
    attested_upload.sftp.files[UPLOAD_PATH] = b"hello"
    probe = AsyncMock(return_value="exact_live")

    removed = await delete_file_from_attested_stateless_workspace(
        attested_upload.thread,
        "notes.txt",
        destination=attested_upload.target,
        expected_workspace_generation=GENERATION,
        expected_runtime_incarnation=RUNTIME,
        expected_host_key_fingerprint=FINGERPRINT,
        authority_probe=probe,
    )

    assert removed == "notes.txt"
    assert UPLOAD_PATH not in attested_upload.sftp.files
    assert probe.await_count == 4
    checkout = attested_upload.pool.checkouts[0]
    assert checkout["thread_id"] == f"{THREAD_ID}:{RUNTIME}"
    assert checkout["generation"] == UUID(GENERATION)
    assert checkout["fingerprint"] == FINGERPRINT


@pytest.mark.asyncio
async def test_attested_delete_reprobes_immediately_before_unlink(attested_upload):
    uploads_dir = "/home/agent-host/workspace/uploads"
    attested_upload.sftp.dirs.add(uploads_dir)
    attested_upload.sftp.files[UPLOAD_PATH] = b"hello"
    probe = AsyncMock(side_effect=["exact_live", "exact_live", "replacement"])

    with pytest.raises(ThreadUploadError) as error:
        await delete_file_from_attested_stateless_workspace(
            attested_upload.thread,
            "notes.txt",
            destination=attested_upload.target,
            expected_workspace_generation=GENERATION,
            expected_runtime_incarnation=RUNTIME,
            expected_host_key_fingerprint=FINGERPRINT,
            authority_probe=probe,
        )

    assert error.value.status_code == 409
    assert attested_upload.sftp.files[UPLOAD_PATH] == b"hello"


@pytest.mark.asyncio
async def test_attested_delete_recurses_with_v4_file_types(attested_upload):
    uploads_dir = "/home/agent-host/workspace/uploads"
    tree = f"{uploads_dir}/bundle"
    nested = f"{tree}/nested"
    leaf = f"{nested}/notes.txt"
    attested_upload.sftp.dirs.update({uploads_dir, tree, nested})
    attested_upload.sftp.files[leaf] = b"hello"

    removed = await delete_file_from_attested_stateless_workspace(
        attested_upload.thread,
        "bundle",
        destination=attested_upload.target,
        expected_workspace_generation=GENERATION,
        expected_runtime_incarnation=RUNTIME,
        expected_host_key_fingerprint=FINGERPRINT,
        authority_probe=AsyncMock(return_value="exact_live"),
    )

    assert removed == "bundle"
    assert leaf not in attested_upload.sftp.files
    assert tree not in attested_upload.sftp.dirs
    assert nested not in attested_upload.sftp.dirs


@pytest.mark.asyncio
async def test_attested_delete_maps_v4_missing_path_to_not_found(attested_upload):
    attested_upload.sftp.lstat = AsyncMock(
        side_effect=asyncssh.SFTPNoSuchPath("missing component")
    )

    removed = await delete_file_from_attested_stateless_workspace(
        attested_upload.thread,
        "notes.txt",
        destination=attested_upload.target,
        expected_workspace_generation=GENERATION,
        expected_runtime_incarnation=RUNTIME,
        expected_host_key_fingerprint=FINGERPRINT,
        authority_probe=AsyncMock(return_value="exact_live"),
    )

    assert removed is None


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["base", "parent"])
async def test_attested_delete_refuses_symlinked_directory_component(
    attested_upload, component
):
    uploads_dir = "/home/agent-host/workspace/uploads"
    parent = f"{uploads_dir}/bundle"
    leaf = f"{parent}/notes.txt"
    attested_upload.sftp.dirs.update({uploads_dir, parent})
    attested_upload.sftp.files[leaf] = b"hello"
    symlink_path = uploads_dir if component == "base" else parent
    attested_upload.sftp.types[symlink_path] = asyncssh.FILEXFER_TYPE_SYMLINK

    with pytest.raises(ThreadUploadError) as error:
        await delete_file_from_attested_stateless_workspace(
            attested_upload.thread,
            "bundle/notes.txt",
            destination=attested_upload.target,
            expected_workspace_generation=GENERATION,
            expected_runtime_incarnation=RUNTIME,
            expected_host_key_fingerprint=FINGERPRINT,
            authority_probe=AsyncMock(return_value="exact_live"),
        )

    assert error.value.status_code == 409
    assert attested_upload.sftp.files[leaf] == b"hello"


@pytest.mark.asyncio
async def test_attested_delete_refuses_unknown_remote_file_type(attested_upload):
    uploads_dir = "/home/agent-host/workspace/uploads"
    attested_upload.sftp.dirs.add(uploads_dir)
    attested_upload.sftp.files[UPLOAD_PATH] = b"hello"
    attested_upload.sftp.types[UPLOAD_PATH] = asyncssh.FILEXFER_TYPE_UNKNOWN

    with pytest.raises(ThreadUploadError) as error:
        await delete_file_from_attested_stateless_workspace(
            attested_upload.thread,
            "notes.txt",
            destination=attested_upload.target,
            expected_workspace_generation=GENERATION,
            expected_runtime_incarnation=RUNTIME,
            expected_host_key_fingerprint=FINGERPRINT,
            authority_probe=AsyncMock(return_value="exact_live"),
        )

    assert error.value.status_code == 409
    assert attested_upload.sftp.files[UPLOAD_PATH] == b"hello"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("argument", "wrong_value"),
    [
        (
            "expected_workspace_generation",
            "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        ),
        (
            "expected_runtime_incarnation",
            "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        ),
        ("expected_host_key_fingerprint", "SHA256:different-host"),
    ],
)
async def test_wrong_exact_attestation_refuses_before_network_or_bytes(
    attested_upload, argument, wrong_value
):
    probe = AsyncMock(return_value="exact_live")

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe, **{argument: wrong_value})

    assert error.value.status_code == 409
    probe.assert_not_awaited()
    assert attested_upload.pool.checkouts == []
    assert attested_upload.sftp.write_attempts == 0
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda thread: thread["metadata"]["_workspace_binding"].pop(
            "ssh_host_key_fingerprint"
        ),
        lambda thread: thread["metadata"]["workspace_container"].pop(
            "_runtime_incarnation"
        ),
        lambda thread: thread["metadata"]["workspace_container"].update(
            {"_snapshot_restore_required": True}
        ),
    ],
    ids=["missing-fingerprint", "missing-runtime", "restore-incomplete"],
)
async def test_missing_or_incomplete_authority_refuses_without_bytes(
    attested_upload, mutation
):
    mutation(attested_upload.thread)
    probe = AsyncMock(return_value="exact_live")

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == 409
    probe.assert_not_awaited()
    assert attested_upload.pool.checkouts == []
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "malformed",
    [None, 0, "", [], {}],
    ids=["null", "zero", "empty-string", "empty-list", "empty-map"],
)
async def test_malformed_restore_marker_refuses_without_bytes(
    attested_upload, malformed
):
    attested_upload.thread["metadata"]["workspace_container"].update(
        {"_snapshot_restore_required": malformed}
    )
    probe = AsyncMock(return_value="exact_live")

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == 409
    probe.assert_not_awaited()
    assert attested_upload.pool.checkouts == []
    assert attested_upload.sftp.write_attempts == 0
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
async def test_exact_false_restore_marker_allows_attested_upload(attested_upload):
    attested_upload.thread["metadata"]["workspace_container"].update(
        {"_snapshot_restore_required": False}
    )
    probe = AsyncMock(return_value="exact_live")

    result = await _upload(attested_upload, probe)

    assert result[0].path == "uploads/notes.txt"
    assert attested_upload.sftp.files == {UPLOAD_PATH: b"hello"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authority_state", "status_code"),
    [
        ("exact_absent", 409),
        ("replacement", 409),
        ("unknown", 503),
        ("future_state", 503),
    ],
)
async def test_non_live_runtime_refuses_before_connection(
    attested_upload, authority_state, status_code
):
    probe = AsyncMock(return_value=authority_state)

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == status_code
    assert attested_upload.pool.checkouts == []
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
async def test_runtime_replacement_after_planning_refuses_before_first_byte(
    attested_upload,
):
    probe = AsyncMock(side_effect=["exact_live", "exact_live", "replacement"])

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == 409
    assert attested_upload.sftp.write_attempts == 0
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
async def test_runtime_replacement_after_write_removes_new_bytes(attested_upload):
    probe = AsyncMock(
        side_effect=["exact_live", "exact_live", "exact_live", "replacement"]
    )

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == 409
    assert attested_upload.sftp.write_attempts == 1
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
async def test_pinned_transport_host_key_rejection_writes_no_bytes(
    attested_upload, monkeypatch
):
    from orchestrator.services import thread_uploads

    rejecting = _RejectingPinnedPool(attested_upload.sftp)
    monkeypatch.setattr(thread_uploads, "_ATTESTED_SFTP_POOL", rejecting)
    probe = AsyncMock(return_value="exact_live")

    with pytest.raises(ThreadUploadError) as error:
        await _upload(attested_upload, probe)

    assert error.value.status_code == 503
    assert rejecting.checkouts[0]["fingerprint"] == FINGERPRINT
    assert attested_upload.sftp.write_attempts == 0
    assert attested_upload.sftp.files == {}


@pytest.mark.asyncio
async def test_pinned_transport_host_key_rejection_deletes_no_bytes(
    attested_upload, monkeypatch
):
    from orchestrator.services import thread_uploads

    attested_upload.sftp.dirs.add("/home/agent-host/workspace/uploads")
    attested_upload.sftp.files[UPLOAD_PATH] = b"hello"
    rejecting = _RejectingPinnedPool(attested_upload.sftp)
    monkeypatch.setattr(thread_uploads, "_ATTESTED_SFTP_POOL", rejecting)

    with pytest.raises(ThreadUploadError) as error:
        await delete_file_from_attested_stateless_workspace(
            attested_upload.thread,
            "notes.txt",
            destination=attested_upload.target,
            expected_workspace_generation=GENERATION,
            expected_runtime_incarnation=RUNTIME,
            expected_host_key_fingerprint=FINGERPRINT,
            authority_probe=AsyncMock(return_value="exact_live"),
        )

    assert error.value.status_code == 503
    assert rejecting.checkouts[0]["fingerprint"] == FINGERPRINT
    assert attested_upload.sftp.files[UPLOAD_PATH] == b"hello"


@pytest.mark.asyncio
async def test_stateless_sandbox_cannot_fall_back_to_legacy_auto_add_path(
    attested_upload, monkeypatch
):
    from orchestrator.services import thread_uploads

    legacy = AsyncMock(side_effect=AssertionError("legacy writer must not run"))
    monkeypatch.setattr(thread_uploads, "_sftp_write_files", legacy)

    with pytest.raises(ThreadUploadError) as error:
        await upload_files_to_thread_workspace(
            attested_upload.thread,
            [("notes.txt", b"hello", "text/plain")],
            destination=attested_upload.target,
        )

    assert error.value.status_code == 409
    legacy.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_sandbox_delete_cannot_fall_back_to_legacy_auto_add_path(
    attested_upload, monkeypatch
):
    from orchestrator.services import thread_uploads

    legacy = MagicMock(side_effect=AssertionError("legacy delete must not run"))
    monkeypatch.setattr(thread_uploads, "_sftp_delete_file", legacy)

    with pytest.raises(ThreadUploadError) as error:
        await delete_file_from_thread_workspace(
            attested_upload.thread,
            "notes.txt",
            destination=attested_upload.target,
        )

    assert error.value.status_code == 409
    legacy.assert_not_called()


@pytest.mark.asyncio
async def test_pinned_legacy_upload_shape_is_unchanged(monkeypatch):
    from orchestrator.services import thread_uploads

    thread = deepcopy(_sandbox_thread())
    thread["execution_lane"] = "pinned"
    target = _SshTarget(
        host="10.42.0.25",
        port=30022,
        username="agent-host",
        key_path="/ssh/key",
        workspace_path="/home/agent-host/workspace",
        host_key_fingerprint="SHA256:" + "a" * 43,
    )
    called: dict = {}

    def legacy_writer(destination, payloads):
        called["destination"] = destination
        called["payloads"] = payloads
        return []

    monkeypatch.setattr(thread_uploads, "_sftp_write_files", legacy_writer)

    assert (
        await upload_files_to_thread_workspace(
            thread,
            [("notes.txt", b"hello", "text/plain")],
            destination=target,
        )
        == []
    )
    assert called == {
        "destination": target,
        "payloads": [("notes.txt", b"hello", "text/plain")],
    }


@pytest.mark.asyncio
async def test_stateless_virtual_upload_remains_on_object_store_path(monkeypatch):
    from orchestrator.services import thread_uploads

    thread = {
        "id": THREAD_ID,
        "execution_lane": "stateless",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    destination = _VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    seen: dict = {}

    def virtual_writer(target, payloads):
        seen["target"] = target
        seen["payloads"] = payloads
        return []

    monkeypatch.setattr(thread_uploads, "_virtual_write_files", virtual_writer)

    assert (
        await upload_files_to_thread_workspace(
            thread,
            [("notes.txt", b"hello", "text/plain")],
            destination=destination,
        )
        == []
    )
    assert seen["target"] == destination


@pytest.mark.asyncio
async def test_cancelled_stateless_virtual_upload_joins_blocking_writer(
    monkeypatch,
):
    from orchestrator.services import thread_uploads

    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def virtual_writer(_target, _payloads):
        entered.set()
        assert release.wait(timeout=5)
        finished.set()
        raise RuntimeError("writer failed after caller cancellation")

    monkeypatch.setattr(thread_uploads, "_virtual_write_files", virtual_writer)
    thread = {
        "id": THREAD_ID,
        "execution_lane": "stateless",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    destination = _VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    uploading = asyncio.create_task(
        upload_files_to_thread_workspace(
            thread,
            [("notes.txt", b"hello", "text/plain")],
            destination=destination,
        )
    )
    deadline = asyncio.get_running_loop().time() + 2
    while not entered.is_set():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)

    uploading.cancel()
    await asyncio.sleep(0.02)
    assert not uploading.done()
    assert not finished.is_set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await uploading
    assert finished.is_set()


@pytest.mark.asyncio
async def test_cancelled_joined_async_effect_wins_over_late_inner_failure():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def effect():
        entered.set()
        await release.wait()
        raise RuntimeError("SFTP failed after caller cancellation")

    running = asyncio.create_task(_joined_async_call(effect()))
    await entered.wait()
    running.cancel()
    await asyncio.sleep(0)
    assert not running.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await running


def test_virtual_prefix_purge_checks_each_delete_and_final_listing() -> None:
    target = _VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    store = SimpleNamespace(
        list=MagicMock(
            side_effect=[
                [SimpleNamespace(key=f"threads/{THREAD_ID}/uploads/a.txt")],
                [],
            ]
        ),
        delete=MagicMock(return_value=True),
    )

    assert _virtual_purge_prefix(target, store=store) is True
    store.delete.assert_called_once_with(f"threads/{THREAD_ID}/uploads/a.txt")
    assert store.list.call_count == 2


@pytest.mark.parametrize("failure", ["delete", "post_list"])
def test_virtual_prefix_purge_fails_closed_on_partial_residue(failure) -> None:
    target = _VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    key = f"threads/{THREAD_ID}/uploads/a.txt"
    store = SimpleNamespace(
        list=MagicMock(
            side_effect=[
                [SimpleNamespace(key=key)],
                [SimpleNamespace(key=key)],
            ]
        ),
        delete=MagicMock(return_value=failure != "delete"),
    )

    assert _virtual_purge_prefix(target, store=store) is False


@pytest.mark.asyncio
async def test_virtual_purge_refuses_backing_drift_before_blocking_effect(
    monkeypatch,
):
    from orchestrator.services import thread_uploads, workspace_binding

    spec = {"type": "s3", "root": "bucket", "config": {}}
    monkeypatch.setattr(
        workspace_binding, "virtual_workspace_rclone_spec", lambda: spec
    )
    monkeypatch.setattr(workspace_binding.shutil, "which", lambda _name: "/rclone")
    blocking = MagicMock(side_effect=AssertionError("drift precedes purge"))
    monkeypatch.setattr(thread_uploads, "_virtual_purge_prefix", blocking)
    thread = {
        "id": THREAD_ID,
        "execution_lane": "stateless",
        "status": "ended",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "virtual",
                "backing_id": "rclone:stale",
                "ssh_host_key_fingerprint": None,
            },
        },
    }

    assert await purge_attested_stateless_virtual_workspace(thread) is False
    blocking.assert_not_called()


def _leftover_store(monkeypatch, spec, *, rclone="/rclone"):
    from orchestrator.services import thread_uploads, workspace_binding

    monkeypatch.setattr(
        workspace_binding, "virtual_workspace_rclone_spec", lambda: spec
    )
    monkeypatch.setattr(workspace_binding.shutil, "which", lambda _name: rclone)
    purge = MagicMock(return_value=True)
    monkeypatch.setattr(thread_uploads, "_virtual_purge_prefix", purge)
    return purge


@pytest.mark.asyncio
async def test_an_upgraded_sessions_old_virtual_prefix_is_purged_by_its_id(
    monkeypatch,
):
    from orchestrator.services.thread_uploads import purge_virtual_workspace_leftover
    from orchestrator.services.workspace_binding import virtual_thread_backing_id

    spec = {"type": "s3", "root": "bucket", "config": {}}
    purge = _leftover_store(monkeypatch, spec)
    recorded = {
        "kind": "virtual",
        "backing_id": virtual_thread_backing_id(THREAD_ID, spec),
    }

    assert await purge_virtual_workspace_leftover(
        {"id": THREAD_ID}, recorded_bindings=[recorded]
    )

    (target,), _ = purge.call_args
    assert (target.spec, target.prefix) == (spec, f"threads/{THREAD_ID}/")


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", [None, {"type": "memory", "root": "", "config": {}}])
async def test_without_a_durable_virtual_store_nothing_is_left(monkeypatch, spec):
    from orchestrator.services.thread_uploads import purge_virtual_workspace_leftover

    purge = _leftover_store(monkeypatch, spec)

    assert await purge_virtual_workspace_leftover({"id": THREAD_ID})
    purge.assert_not_called()


@pytest.mark.asyncio
async def test_a_recorded_binding_on_another_store_fails_closed(monkeypatch):
    from orchestrator.services.thread_uploads import purge_virtual_workspace_leftover

    purge = _leftover_store(monkeypatch, {"type": "s3", "root": "bucket", "config": {}})

    assert not await purge_virtual_workspace_leftover(
        {"id": THREAD_ID},
        recorded_bindings=[{"kind": "virtual", "backing_id": "rclone:elsewhere"}],
    )
    purge.assert_not_called()


@pytest.mark.asyncio
async def test_a_leftover_purge_without_rclone_or_with_residue_reports_false(
    monkeypatch,
):
    from orchestrator.services.thread_uploads import purge_virtual_workspace_leftover

    spec = {"type": "s3", "root": "bucket", "config": {}}
    purge = _leftover_store(monkeypatch, spec, rclone=None)
    assert not await purge_virtual_workspace_leftover({"id": THREAD_ID})
    purge.assert_not_called()

    purge = _leftover_store(monkeypatch, spec)
    purge.return_value = False
    assert not await purge_virtual_workspace_leftover({"id": THREAD_ID})
    purge.side_effect = RuntimeError("store down")
    assert not await purge_virtual_workspace_leftover({"id": THREAD_ID})


def test_the_recorded_virtual_bindings_of_an_upgrade_are_collected():
    from orchestrator.services.thread_uploads import recorded_virtual_upgrade_bindings

    c1 = {"kind": "virtual", "backing_id": "rclone:c1"}
    c2 = {"kind": "virtual", "backing_id": "rclone:c2"}
    pinned = {"kind": "virtual", "backing_id": "rclone:pinned"}
    legacy = {"kind": "virtual", "backing_id": "rclone:legacy"}
    metadata = {
        "workspace_container": {"seed_from": {"backend": "virtual", "binding": c1}},
        "vm": {
            "upgrade_seed": {"source": {"backend": "virtual", "binding": c2}},
            "upgrade_source": {"binding": {"kind": "remote", "backing_id": "x"}},
        },
        "_workspace_binding": legacy,
    }

    assert recorded_virtual_upgrade_bindings(metadata) == [c1, c2, legacy]
    assert recorded_virtual_upgrade_bindings(
        {}, vm={"upgrade_source": {"binding": pinned}}
    ) == [pinned]
    assert recorded_virtual_upgrade_bindings({"workspace_container": "junk"}) == []
