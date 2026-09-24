import asyncio
import threading
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import container_provisioner as container_provisioner_module


THREAD_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
GENERATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
RUNTIME = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
REPLACEMENT_RUNTIME = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
AGENT_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
FINGERPRINT = "SHA256:route-pinned"


def _thread():
    return {
        "id": THREAD_ID,
        "user_id": "user-a",
        "execution_lane": "stateless",
        "status": "active",
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.42.0.8",
                "port": 30022,
                "pod_name": "ws-thread-a",
                "namespace": "workspaces",
                "_canvas_workspace_generation": GENERATION,
                "_runtime_incarnation": RUNTIME,
            },
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "remote",
                "backing_id": "k8s-pod:workspace-a",
                "ssh_host_key_fingerprint": FINGERPRINT,
            },
        },
    }


def _pinned_thread():
    thread = _thread()
    thread.update(
        {
            "execution_lane": "pinned",
            "agent_id": AGENT_ID,
            "runtime_generation": "ffffffff-ffff-4fff-8fff-fffffffffff1",
            "runtime_retirement_token": None,
        }
    )
    thread["metadata"]["_workspace_binding"]["backing_id"] = (
        f"k8s-pod:workspaces:{GENERATION}"
    )
    return thread


def _workspace_attestation(runtime=RUNTIME):
    return SimpleNamespace(
        backing_id=f"k8s-pod:workspaces:{GENERATION}",
        workspace_generation=GENERATION,
        runtime_incarnation=runtime,
        ssh_host_key_fingerprint=FINGERPRINT,
        host="10.42.0.8",
        pod_ip="10.42.0.8",
        port=30022,
    )


class _PinnedDB:
    def __init__(self, thread):
        self.thread = thread
        self.binding = SimpleNamespace(
            agent_id=AGENT_ID,
            agent_status="session",
            target_key=(
                THREAD_ID,
                thread["runtime_generation"],
                AGENT_ID,
                "attach-token",
                "agent-pod",
                "agent-pod-uid",
                "10.42.0.50",
                8001,
            ),
        )
        self.get_thread = AsyncMock(side_effect=self._get_thread)
        self.get_pinned_session_binding = AsyncMock(return_value=self.binding)

    async def _get_thread(self, _thread_id):
        return deepcopy(self.thread)


class _Upload:
    filename = "notes.txt"
    content_type = "text/plain"

    def __init__(self, order):
        self.order = order

    async def read(self):
        self.order.append("read")
        return b"hello"


class _DB:
    def __init__(self, thread, order):
        self.thread = thread
        self.order = order
        self.get_thread = AsyncMock(side_effect=self._get_thread)

    async def _get_thread(self, _thread_id):
        self.order.append("fresh")
        return self.thread

    @asynccontextmanager
    async def stateless_session_workspace_ensure_lock(self, _thread_id, **_kwargs):
        self.order.append("lock")
        try:
            yield True
        finally:
            self.order.append("unlock")


def _dependencies(db, owner):
    """The application's own wiring with the two patched collaborators swapped.

    ``postgres_db`` and ``require_thread_owner`` used to be ``main`` globals
    these tests patched; they are ``ThreadFilesDependencies`` fields now. The
    rest (both provisioners, the backend/lane readers) stays exactly what the
    application binds, so ``patch.object(main.container_provisioner, ...)``
    still reaches the object under test.
    """
    from dataclasses import replace

    from orchestrator import main

    return replace(
        workspace_composition.thread_files_dependencies(main.app.state.resources),
        store=db,
        require_thread_owner=owner,
    )


@pytest.mark.asyncio
async def test_pinned_k8s_upload_requires_fresh_exact_attestation():
    from orchestrator.routers.thread_files import upload_files_to_thread
    from orchestrator.services import thread_uploads

    thread = _pinned_thread()
    db = _PinnedDB(thread)

    async def _write(*_args, **kwargs):
        assert await kwargs["authority_probe"]() == "exact_live"
        return [
            SimpleNamespace(
                name="notes.txt",
                size=5,
                mime_type="text/plain",
                path="uploads/notes.txt",
            )
        ]

    with (
        patch.object(thread_uploads, "resolve_ssh_key_path", return_value="/ssh/key"),
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=_workspace_attestation()),
        ) as attest,
        patch.object(
            thread_uploads,
            "upload_files_to_attested_k8s_workspace",
            AsyncMock(side_effect=_write),
        ) as writer,
    ):
        result = await upload_files_to_thread(
            THREAD_ID,
            SimpleNamespace(),
            [_Upload([])],
            dependencies=_dependencies(
                db, AsyncMock(return_value=({"sub": "user-a"}, thread))
            ),
        )

    assert result["files"][0]["path"] == "uploads/notes.txt"
    assert attest.await_count == 1
    assert writer.await_args.kwargs["expected_runtime_incarnation"] == RUNTIME
    assert writer.await_args.kwargs["expected_host_key_fingerprint"] == FINGERPRINT


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "delete"])
async def test_pinned_k8s_same_ip_successor_gets_no_legacy_io(operation):
    from orchestrator.routers.thread_files import (
        delete_thread_upload,
        upload_files_to_thread,
    )
    from orchestrator.services import thread_uploads

    thread = _pinned_thread()
    db = _PinnedDB(thread)
    legacy_upload = AsyncMock(side_effect=AssertionError("legacy upload reached"))
    legacy_delete = AsyncMock(side_effect=AssertionError("legacy delete reached"))

    async def _refuse_before_io(*_args, **kwargs):
        assert await kwargs["authority_probe"]() == "replacement"
        raise thread_uploads.ThreadUploadError(409, "runtime replaced")

    with (
        patch.object(thread_uploads, "resolve_ssh_key_path", return_value="/ssh/key"),
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=_workspace_attestation(REPLACEMENT_RUNTIME)),
        ),
        patch.object(
            thread_uploads,
            "upload_files_to_attested_k8s_workspace",
            AsyncMock(side_effect=_refuse_before_io),
        ) as attested_upload,
        patch.object(
            thread_uploads,
            "delete_file_from_attested_k8s_workspace",
            AsyncMock(side_effect=_refuse_before_io),
        ) as attested_delete,
        patch.object(
            thread_uploads,
            "upload_files_to_thread_workspace",
            legacy_upload,
        ),
        patch.object(
            thread_uploads,
            "delete_file_from_thread_workspace",
            legacy_delete,
        ),
    ):
        dependencies = _dependencies(
            db, AsyncMock(return_value=({"sub": "user-a"}, thread))
        )
        with pytest.raises(HTTPException) as error:
            if operation == "upload":
                await upload_files_to_thread(
                    THREAD_ID,
                    SimpleNamespace(),
                    [_Upload([])],
                    dependencies=dependencies,
                )
            else:
                await delete_thread_upload(
                    THREAD_ID,
                    "notes.txt",
                    SimpleNamespace(),
                    dependencies=dependencies,
                )

    assert error.value.status_code == 409
    legacy_upload.assert_not_awaited()
    legacy_delete.assert_not_awaited()
    if operation == "upload":
        attested_upload.assert_awaited_once()
        attested_delete.assert_not_awaited()
    else:
        attested_delete.assert_awaited_once()
        attested_upload.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_upload_holds_lifecycle_lock_through_final_write():
    from orchestrator.routers.thread_files import upload_files_to_thread
    from orchestrator.services import thread_uploads

    order = []
    thread = _thread()
    db = _DB(thread, order)

    async def _write(*_args, **kwargs):
        order.append("write")
        assert await kwargs["authority_probe"]() == "exact_live"
        return [
            SimpleNamespace(
                name="notes.txt",
                size=5,
                mime_type="text/plain",
                path="uploads/notes.txt",
            )
        ]

    with (
        patch.object(thread_uploads, "resolve_ssh_key_path", return_value="/ssh/key"),
        patch.object(
            container_provisioner_module.container_provisioner,
            "workspace_pod_authority",
            AsyncMock(return_value="exact_live"),
        ),
        patch.object(
            thread_uploads,
            "upload_files_to_attested_stateless_workspace",
            AsyncMock(side_effect=_write),
        ) as writer,
    ):
        result = await upload_files_to_thread(
            THREAD_ID,
            SimpleNamespace(),
            [_Upload(order)],
            dependencies=_dependencies(
                db, AsyncMock(return_value=({"sub": "user-a"}, thread))
            ),
        )

    assert result["files"][0]["path"] == "uploads/notes.txt"
    assert order == ["lock", "fresh", "read", "write", "unlock"]
    assert writer.await_args.kwargs["expected_workspace_generation"] == GENERATION
    assert writer.await_args.kwargs["expected_runtime_incarnation"] == RUNTIME
    assert writer.await_args.kwargs["expected_host_key_fingerprint"] == FINGERPRINT


@pytest.mark.asyncio
async def test_retirement_marker_wins_before_upload_materialization():
    from orchestrator.routers.thread_files import upload_files_to_thread
    from orchestrator.services import thread_uploads

    order = []
    initial = _thread()
    ended = deepcopy(initial)
    ended["status"] = "ended"
    ended["metadata"]["_stateless_workspace_retirement_pending"] = True
    db = _DB(ended, order)
    upload = _Upload(order)

    with patch.object(
        thread_uploads,
        "upload_files_to_attested_stateless_workspace",
        AsyncMock(),
    ) as writer:
        with pytest.raises(HTTPException) as exc:
            await upload_files_to_thread(
                THREAD_ID,
                SimpleNamespace(),
                [upload],
                dependencies=_dependencies(
                    db, AsyncMock(return_value=({"sub": "user-a"}, initial))
                ),
            )

    assert exc.value.status_code == 409
    assert order == ["lock", "fresh", "unlock"]
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_delete_holds_lifecycle_lock_through_exact_delete():
    from orchestrator.routers.thread_files import delete_thread_upload
    from orchestrator.services import thread_uploads

    order: list[str] = []
    thread = _thread()
    db = _DB(thread, order)

    async def _delete(*_args, **kwargs):
        order.append("delete")
        assert await kwargs["authority_probe"]() == "exact_live"
        return "notes.txt"

    with (
        patch.object(thread_uploads, "resolve_ssh_key_path", return_value="/ssh/key"),
        patch.object(
            container_provisioner_module.container_provisioner,
            "workspace_pod_authority",
            AsyncMock(return_value="exact_live"),
        ),
        patch.object(
            thread_uploads,
            "delete_file_from_attested_stateless_workspace",
            AsyncMock(side_effect=_delete),
        ) as deleter,
    ):
        result = await delete_thread_upload(
            THREAD_ID,
            "notes.txt",
            SimpleNamespace(),
            dependencies=_dependencies(
                db, AsyncMock(return_value=({"sub": "user-a"}, thread))
            ),
        )

    assert result == {
        "thread_id": THREAD_ID,
        "path": "uploads/notes.txt",
        "deleted": True,
    }
    assert order == ["lock", "fresh", "delete", "unlock"]
    assert deleter.await_args.kwargs["expected_workspace_generation"] == GENERATION
    assert deleter.await_args.kwargs["expected_runtime_incarnation"] == RUNTIME
    assert deleter.await_args.kwargs["expected_host_key_fingerprint"] == FINGERPRINT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_key",
    [
        "_stateless_workspace_retirement_pending",
        "_stateless_claim_retirement",
        "_stateless_claim_loss_hold",
        "_stateless_claim_losses",
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_present_falsey_stop_marker_refuses_delete_before_transport(
    marker_key, value
):
    from orchestrator.routers.thread_files import delete_thread_upload
    from orchestrator.services import thread_uploads

    order: list[str] = []
    initial = _thread()
    blocked = deepcopy(initial)
    blocked["metadata"][marker_key] = value
    db = _DB(blocked, order)

    with patch.object(
        thread_uploads,
        "delete_file_from_attested_stateless_workspace",
        AsyncMock(),
    ) as deleter:
        with pytest.raises(HTTPException) as exc:
            await delete_thread_upload(
                THREAD_ID,
                "notes.txt",
                SimpleNamespace(),
                dependencies=_dependencies(
                    db, AsyncMock(return_value=({"sub": "user-a"}, initial))
                ),
            )

    assert exc.value.status_code == 409
    assert order == ["lock", "fresh", "unlock"]
    deleter.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marker_key",
    [
        "_stateless_workspace_retirement_pending",
        "_stateless_claim_retirement",
        "_stateless_claim_loss_hold",
        "_stateless_claim_losses",
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_present_falsey_stop_marker_refuses_upload_before_read(marker_key, value):
    from orchestrator.routers.thread_files import upload_files_to_thread
    from orchestrator.services import thread_uploads

    order = []
    initial = _thread()
    blocked = deepcopy(initial)
    blocked["metadata"][marker_key] = value
    db = _DB(blocked, order)

    with patch.object(
        thread_uploads,
        "upload_files_to_attested_stateless_workspace",
        AsyncMock(),
    ) as writer:
        with pytest.raises(HTTPException) as exc:
            await upload_files_to_thread(
                THREAD_ID,
                SimpleNamespace(),
                [_Upload(order)],
                dependencies=_dependencies(
                    db, AsyncMock(return_value=({"sub": "user-a"}, initial))
                ),
            )

    assert exc.value.status_code == 409
    assert order == ["lock", "fresh", "unlock"]
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancelled_virtual_upload_keeps_lifecycle_lock_until_writer_finishes():
    from orchestrator.routers.thread_files import upload_files_to_thread
    from orchestrator.services import thread_uploads

    order: list[str] = []
    entered = threading.Event()
    release = threading.Event()
    lock = asyncio.Lock()
    thread = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "execution_lane": "stateless",
        "status": "active",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "virtual",
                "backing_id": f"rclone:threads/{THREAD_ID}",
                "ssh_host_key_fingerprint": None,
            },
        },
    }

    class _LockDB:
        get_thread = AsyncMock(return_value=thread)

        @asynccontextmanager
        async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
            async with lock:
                order.append("upload-lock")
                try:
                    yield True
                finally:
                    order.append("upload-unlock")

    def _blocked_write(_target, _payloads):
        entered.set()
        assert release.wait(timeout=5)
        order.append("write-finished")
        return []

    destination = thread_uploads._VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    db = _LockDB()
    with (
        patch.object(
            thread_uploads,
            "resolve_thread_upload_destination",
            return_value=destination,
        ),
        patch.object(thread_uploads, "_virtual_write_files", _blocked_write),
    ):
        uploading = asyncio.create_task(
            upload_files_to_thread(
                THREAD_ID,
                SimpleNamespace(),
                [_Upload(order)],
                dependencies=_dependencies(
                    db, AsyncMock(return_value=({"sub": "user-a"}, thread))
                ),
            )
        )
        deadline = asyncio.get_running_loop().time() + 2
        while not entered.is_set():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)

        uploading.cancel()

        async def _end_owner():
            async with db.stateless_session_workspace_ensure_lock(THREAD_ID):
                order.append("end-delete")

        ending = asyncio.create_task(_end_owner())
        await asyncio.sleep(0.02)
        assert not uploading.done()
        assert not ending.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await uploading
        await ending

    assert order.index("write-finished") < order.index("upload-unlock")
    assert order.index("upload-unlock") < order.index("end-delete")


@pytest.mark.asyncio
async def test_cancelled_virtual_delete_keeps_lifecycle_lock_until_worker_finishes():
    from orchestrator.routers.thread_files import delete_thread_upload
    from orchestrator.services import thread_uploads

    order: list[str] = []
    entered = threading.Event()
    release = threading.Event()
    lock = asyncio.Lock()
    thread = {
        "id": THREAD_ID,
        "user_id": "user-a",
        "execution_lane": "stateless",
        "status": "active",
        "metadata": {
            "config_override": {"workspace": {"backend": "virtual"}},
            "_workspace_binding": {
                "generation": GENERATION,
                "kind": "virtual",
                "backing_id": f"rclone:threads/{THREAD_ID}",
                "ssh_host_key_fingerprint": None,
            },
        },
    }

    class _LockDB:
        get_thread = AsyncMock(return_value=thread)

        @asynccontextmanager
        async def stateless_session_workspace_ensure_lock(self, *_args, **_kwargs):
            async with lock:
                order.append("delete-lock")
                try:
                    yield True
                finally:
                    order.append("delete-unlock")

    def _blocked_delete(_target, _relpath):
        entered.set()
        assert release.wait(timeout=5)
        order.append("delete-finished")
        return True

    destination = thread_uploads._VirtualTarget(
        spec={"type": "s3", "root": "bucket", "config": {}},
        prefix=f"threads/{THREAD_ID}/",
    )
    db = _LockDB()
    with (
        patch.object(
            thread_uploads,
            "resolve_thread_upload_destination",
            return_value=destination,
        ),
        patch.object(thread_uploads, "_virtual_delete_file", _blocked_delete),
    ):
        deleting = asyncio.create_task(
            delete_thread_upload(
                THREAD_ID,
                "notes.txt",
                SimpleNamespace(),
                dependencies=_dependencies(
                    db, AsyncMock(return_value=({"sub": "user-a"}, thread))
                ),
            )
        )
        deadline = asyncio.get_running_loop().time() + 2
        while not entered.is_set():
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)

        deleting.cancel()

        async def _resume_owner():
            async with db.stateless_session_workspace_ensure_lock(THREAD_ID):
                order.append("resume")

        resuming = asyncio.create_task(_resume_owner())
        await asyncio.sleep(0.02)
        assert not deleting.done()
        assert not resuming.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await deleting
        await resuming

    assert order.index("delete-finished") < order.index("delete-unlock")
    assert order.index("delete-unlock") < order.index("resume")
