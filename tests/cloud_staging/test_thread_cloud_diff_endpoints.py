"""Thread cloud-diff read endpoints — summary, per-file, restage (Task 8).

Owner-facing review surface for protected cloud mode (Slice C): ``GET
.../cloud-diff`` (summary), ``GET .../cloud-diff/{path}`` (per-file content),
``POST .../cloud-diff/restage``. All three share one gate — 404 unless the
thread is protected (``metadata.protected_cloud``) AND
``PROTECTED_CLOUD_MODE_ENABLED`` — and one resolver
(``thread_cloud_diff._thread_cloud_diff_source``) that builds a real
``UpperdirDiffSource``
(Task 7) off a fake S3 manifest + tar via a patched ``snapshot_service``, so
these tests exercise the actual diff-source logic rather than re-mocking it.

Spec §11: reads must work for ENDED threads too — a mount row can be
``status='revoked'`` (grant already reconciled away) while
``staged_summary`` is still present, and review must still work. Only
restage needs a live workspace.

Follows the house pattern in tests/test_job_diff_endpoints.py and
tests/cloud_staging/test_stage_triggers.py: ``import main`` (conftest puts
orchestrator/ on sys.path), ExitStack-patch its module globals
(``postgres_db`` / ``snapshot_service`` / ``main_cloud_router``), build the
route dependencies from main's own factory inside those patches, and call the
endpoint coroutines directly.

R1.B04 moved the routes to ``routers.thread_cloud_diff`` and the gate and
resolver to ``services.thread_cloud_diff``. The owner gate is a declared
dependency now, so it is injected rather than patched onto ``main``.
"""

import hashlib
import io
import json
import tarfile
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import thread_cloud_diff as diff_routes
from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from tests.cloud.fake import FakeMainCloudBackend
from orchestrator.application import workspace as workspace_composition
from orchestrator.services import snapshot_service as snapshot_service_module

THREAD_ID = "thread-cd-1"
_RUNTIME_GENERATION = "44444444-4444-4444-8444-444444444444"
_AGENT_ID = "55555555-5555-4555-8555-555555555555"
_ATTACH_TOKEN = "66666666-6666-4666-8666-666666666666"
_WORKSPACE_GENERATION = "77777777-7777-4777-8777-777777777777"
_WORKSPACE_RUNTIME = "88888888-8888-4888-8888-888888888888"
_SOURCE = ProtectedMountSourceIdentity(
    backend_instance_id="11111111-1111-4111-8111-111111111111",
    source_ref="22222222-2222-4222-8222-222222222222",
    target_path="MyProject",
    native_id="1",
    mountpoint="MyProject",
)
_PLAN = ProtectedNextcloudReaderGrantPlan(
    engage_attempt="33333333-3333-4333-8333-333333333333",
    backend_instance_id=_SOURCE.backend_instance_id,
    source=_SOURCE,
)


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #


def _make_user() -> dict:
    return {
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "email": "user@example.test",
    }


def _make_thread(
    *, protected: bool = True, workspace: bool = True, **meta_over
) -> dict:
    metadata = {"protected_cloud": protected}
    if workspace:
        metadata["workspace_container"] = {
            "status": "ready",
            "pod_ip": "10.0.0.5",
            "port": 30022,
            "_canvas_workspace_generation": _WORKSPACE_GENERATION,
            "_runtime_incarnation": _WORKSPACE_RUNTIME,
        }
        metadata["_workspace_binding"] = {
            "generation": _WORKSPACE_GENERATION,
            "kind": "remote",
            "ssh_host_key_fingerprint": "SHA256:trusted",
        }
    metadata.update(meta_over)
    return {
        "id": THREAD_ID,
        "user_id": _make_user()["id"],
        "status": "active",
        "execution_lane": "pinned",
        "runtime_generation": _RUNTIME_GENERATION,
        "runtime_retirement_token": None,
        "runtime_retirement_authorized_at": None,
        "runtime_authority_exposed": True,
        "agent_id": _AGENT_ID,
        "runtime_attach_token": _ATTACH_TOKEN,
        "metadata": metadata,
    }


def _build_tar(members: list[tuple[str, bytes]]) -> bytes:
    """Same helper pattern as test_upperdir_diff_source.py::_build_tar."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for name, data in members:
            ti = tarfile.TarInfo(name=name)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def _manifest(
    entries: list[dict], *, tar_bytes: bytes, epoch=3, staged_at="2026-07-12T00:00:00Z"
) -> dict:
    counts = {"added": 0, "modified": 0, "deleted": 0}
    for e in entries:
        counts[e["status"]] += 1
    return {
        "epoch": epoch,
        "staged_at": staged_at,
        "counts": counts,
        "entries": entries,
        "skipped": [],
        "tar_sha256": hashlib.sha256(tar_bytes).hexdigest(),
        "source_binding": _SOURCE.binding,
        "source_binding_sha256": _SOURCE.sha256,
    }


def _mount_row(*, staged_summary=None, status="active", backend="nextcloud") -> dict:
    if isinstance(staged_summary, dict):
        staged_summary = dict(staged_summary)
        staged_summary.setdefault("source_binding", _SOURCE.binding)
        staged_summary.setdefault("source_binding_sha256", _SOURCE.sha256)
    return {
        "id": "mount-1",
        "thread_id": THREAD_ID,
        "user_id": _make_user()["id"],
        "status": status,
        "backend": backend,
        "backend_instance_id": _SOURCE.backend_instance_id,
        "reader_id": _PLAN.reader_id,
        "grant_group_id": _PLAN.group_id,
        "grant_handle": _PLAN.grant_handle,
        "grant_handle_sha256": _PLAN.grant_handle_sha256,
        "runtime_generation": _RUNTIME_GENERATION,
        "engage_attempt": _PLAN.engage_attempt,
        "selected_mount_id": "55555555-5555-4555-8555-555555555555",
        "source_binding": _SOURCE.binding,
        "source_binding_sha256": _SOURCE.sha256,
        "staged_summary": staged_summary,
    }


def _thread_mounts_row(*, mountpoint="MyProject", cloud_handle="proj-1") -> dict:
    # Real ``thread_mounts`` rows use backend_id/cloud_handle (verified against
    # postgres.list_thread_mounts' SELECT) — select_protected_mount matches on
    # those two keys. ``mountpoint`` is NOT a real column (real rows carry
    # ``target_path`` instead); it's included here to drive the display-name
    # resolution the same way tests/cloud_staging/test_manifest.py does.
    return {
        "backend_id": "nextcloud",
        "cloud_handle": cloud_handle,
        "mountpoint": mountpoint,
    }


def _snapshot_service(blobs: dict[str, bytes] | None = None):
    svc = MagicMock()

    async def _get_blob(key, *, max_bytes=None):
        blob = (blobs or {}).get(key)
        if blob is not None and max_bytes is not None and len(blob) > max_bytes:
            return None
        return blob

    async def _download_blob_file(key, local_path, *, max_bytes):
        blob = (blobs or {}).get(key)
        if blob is None or len(blob) > max_bytes:
            return False
        Path(local_path).write_bytes(blob)
        return True

    svc.get_blob = AsyncMock(side_effect=_get_blob)
    svc.download_blob_file = AsyncMock(side_effect=_download_blob_file)
    return svc


def _cloud_router(backend):
    router = MagicMock()
    router.for_backend_instance = MagicMock(return_value=backend)
    return router


@contextmanager
def _patch_endpoint(
    *,
    user: dict,
    thread: dict,
    ro_mount_row: dict | None = None,
    thread_mounts: list[dict] | None = None,
    snapshot_service=None,
    backend=None,
    require_thread_owner_result=None,
):
    """Patch every global the three endpoints touch; yield db + dependencies.

    ``require_thread_owner`` is a declared field on
    ``ThreadCloudDiffRouteDependencies`` (defaulted to the real
    ``security.access`` gate, which main's factory leaves alone), so it is
    injected with ``dataclasses.replace``. The rest are still main globals, and
    ``main._thread_cloud_diff_dependencies()`` reads them live — which is why
    it is called inside this stack.
    """
    stack = ExitStack()
    gate = (
        require_thread_owner_result
        if require_thread_owner_result is not None
        else AsyncMock(return_value=(user, thread))
    )
    db = MagicMock()
    db.get_ro_mount_by_thread = AsyncMock(return_value=ro_mount_row)
    db.list_thread_mounts = AsyncMock(return_value=thread_mounts or [])
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    stack.enter_context(
        patch(
            "orchestrator.services.snapshot_service.snapshot_service",
            snapshot_service or _snapshot_service(),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.main.app.state.resources.main_cloud_router",
            _cloud_router(backend if backend is not None else FakeMainCloudBackend()),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.deployment_gates.is_protected_cloud_mode_enabled",
            lambda: True,
        )
    )
    with stack:
        dependencies = replace(
            workspace_composition.thread_cloud_diff_dependencies(
                orchestrator.main.app.state.resources
            ),
            require_thread_owner=gate,
        )
        # Proof the patches intercept: the factory resolves these globals at
        # call time, and protected mode is env-off by default, so a missed
        # patch would 404 every protected case below.
        assert dependencies.operations.store is db
        assert dependencies.operations.is_protected_cloud_mode_enabled() is True
        assert dependencies.require_thread_owner is gate
        yield SimpleNamespace(db=db, dependencies=dependencies, gate=gate)


# --------------------------------------------------------------------------- #
# GET /api/agents/threads/{thread_id}/cloud-diff  (summary)
# --------------------------------------------------------------------------- #


class TestCloudDiffSummary:
    @pytest.mark.asyncio
    async def test_summary_returns_counts_epoch_and_files(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        tar_bytes = _build_tar(
            [("upper/new.txt", b"hello"), ("upper/mod.txt", b"world")]
        )
        entries = [
            {"path": "new.txt", "status": "added", "size": 5, "binary": False},
            {"path": "mod.txt", "status": "modified", "size": 5, "binary": False},
        ]
        manifest = _manifest(entries, tar_bytes=tar_bytes, epoch=7, staged_at="ts-7")
        row = _mount_row(
            staged_summary={"signature": "sig", "tar_sha256": "irrelevant"}
        )
        svc = _snapshot_service(
            {
                f"cloud-staging/{THREAD_ID}/manifest.json": json.dumps(
                    manifest
                ).encode(),
                f"cloud-staging/{THREAD_ID}/upper.tar": tar_bytes,
            }
        )
        with _patch_endpoint(
            user=user,
            thread=thread,
            ro_mount_row=row,
            # The mutable current selection is B. Review must retain A's
            # immutable source label from the staged authority.
            thread_mounts=[
                _thread_mounts_row(mountpoint="ReplacementB", cloud_handle="99")
            ],
            snapshot_service=svc,
        ) as wired:
            result = await diff_routes.get_thread_cloud_diff_summary(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )

        assert result == {
            "thread_id": THREAD_ID,
            "epoch": 7,
            "staged_at": "ts-7",
            "counts": {"added": 1, "modified": 1, "deleted": 0},
            "protected_mount": "MyProject",
            "files": [
                {"path": "new.txt", "status": "added", "binary": False},
                {"path": "mod.txt", "status": "modified", "binary": False},
            ],
        }

    @pytest.mark.asyncio
    async def test_summary_empty_when_nothing_staged(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        row = _mount_row(staged_summary=None)
        with _patch_endpoint(
            user=user,
            thread=thread,
            ro_mount_row=row,
            thread_mounts=[_thread_mounts_row()],
        ) as wired:
            result = await diff_routes.get_thread_cloud_diff_summary(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )

        assert result == {
            "thread_id": THREAD_ID,
            "epoch": 0,
            "staged_at": None,
            "counts": {"added": 0, "modified": 0, "deleted": 0},
            "protected_mount": "MyProject",
            "files": [],
        }

    @pytest.mark.asyncio
    async def test_summary_404_when_thread_not_protected(self, fake_request):
        user = _make_user()
        thread = _make_thread(protected=False)
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.get_thread_cloud_diff_summary(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )
        assert ei.value.status_code == 404

    @pytest.mark.asyncio
    async def test_summary_works_on_revoked_mount_row(self, fake_request):
        """Ended thread: mount row status='revoked' but staged_summary set —
        review must still work (spec §11); the resolver never checks status."""
        user = _make_user()
        thread = _make_thread(workspace=False)  # ended thread has no live pod
        tar_bytes = _build_tar([("upper/del.txt", b"")])
        entries = [
            {"path": "gone.txt", "status": "deleted", "size": 0, "binary": False}
        ]
        manifest = _manifest(
            entries, tar_bytes=tar_bytes, epoch=4, staged_at="ts-ended"
        )
        row = _mount_row(
            status="revoked",
            staged_summary={"signature": "sig", "tar_sha256": "irrelevant"},
        )
        svc = _snapshot_service(
            {
                f"cloud-staging/{THREAD_ID}/manifest.json": json.dumps(
                    manifest
                ).encode(),
                f"cloud-staging/{THREAD_ID}/upper.tar": tar_bytes,
            }
        )
        with _patch_endpoint(
            user=user,
            thread=thread,
            ro_mount_row=row,
            thread_mounts=[_thread_mounts_row()],
            snapshot_service=svc,
        ) as wired:
            result = await diff_routes.get_thread_cloud_diff_summary(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )

        assert result["epoch"] == 4
        assert result["counts"] == {"added": 0, "modified": 0, "deleted": 1}
        assert result["files"] == [
            {"path": "gone.txt", "status": "deleted", "binary": False}
        ]


# --------------------------------------------------------------------------- #
# GET /api/agents/threads/{thread_id}/cloud-diff/{file_path:path}
# --------------------------------------------------------------------------- #


class TestCloudDiffFile:
    @pytest.mark.asyncio
    async def test_file_returns_old_new_content(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        tar_bytes = _build_tar([("upper/mod.txt", b"newv")])
        entries = [
            {"path": "mod.txt", "status": "modified", "size": 4, "binary": False}
        ]
        manifest = _manifest(entries, tar_bytes=tar_bytes)
        row = _mount_row(
            staged_summary={"signature": "sig", "tar_sha256": "irrelevant"}
        )
        svc = _snapshot_service(
            {
                f"cloud-staging/{THREAD_ID}/manifest.json": json.dumps(
                    manifest
                ).encode(),
                f"cloud-staging/{THREAD_ID}/upper.tar": tar_bytes,
            }
        )
        backend = FakeMainCloudBackend()
        backend.seed_project_file(_SOURCE.native_id, "mod.txt", b"oldv")
        with _patch_endpoint(
            user=user,
            thread=thread,
            ro_mount_row=row,
            thread_mounts=[_thread_mounts_row(cloud_handle="proj-1")],
            snapshot_service=svc,
            backend=backend,
        ) as wired:
            result = await diff_routes.get_thread_cloud_diff_file(
                THREAD_ID, "mod.txt", fake_request, dependencies=wired.dependencies
            )

        assert result == {
            "thread_id": THREAD_ID,
            "path": "mod.txt",
            "status": "modified",
            "old_content": "oldv",
            "new_content": "newv",
            "old_binary": False,
            "new_binary": False,
        }

    @pytest.mark.asyncio
    async def test_file_404_for_unknown_path(self, fake_request):
        user = _make_user()
        thread = _make_thread()
        tar_bytes = _build_tar([("upper/mod.txt", b"newv")])
        entries = [
            {"path": "mod.txt", "status": "modified", "size": 4, "binary": False}
        ]
        manifest = _manifest(entries, tar_bytes=tar_bytes)
        row = _mount_row(
            staged_summary={"signature": "sig", "tar_sha256": "irrelevant"}
        )
        svc = _snapshot_service(
            {
                f"cloud-staging/{THREAD_ID}/manifest.json": json.dumps(
                    manifest
                ).encode(),
                f"cloud-staging/{THREAD_ID}/upper.tar": tar_bytes,
            }
        )
        with (
            _patch_endpoint(
                user=user,
                thread=thread,
                ro_mount_row=row,
                thread_mounts=[_thread_mounts_row()],
                snapshot_service=svc,
            ) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.get_thread_cloud_diff_file(
                THREAD_ID,
                "does-not-exist.txt",
                fake_request,
                dependencies=wired.dependencies,
            )
        assert ei.value.status_code == 404
        # The code is what lets the review UI say which of the three
        # situations this is instead of guessing "the session re-staged".
        assert ei.value.detail["code"] == "not_in_staged_diff"

    @pytest.mark.asyncio
    async def test_file_404_distinguishes_unreadable_staged_content(self, fake_request):
        """A path the manifest lists but whose tar cannot be trusted.

        ``_get_tar()`` refuses a tar whose sha256 does not match the
        manifest's ``tar_sha256`` (the torn manifest/tar pair defence), so
        ``file()`` returns None for an entry that IS staged. Reported as
        re-staging, this reads as "your session moved on"; it is actually
        "the staged copy is unusable", and only a restage fixes it.
        """
        user = _make_user()
        thread = _make_thread()
        tar_bytes = _build_tar([("upper/mod.txt", b"newv")])
        entries = [
            {"path": "mod.txt", "status": "modified", "size": 4, "binary": False}
        ]
        manifest = _manifest(entries, tar_bytes=tar_bytes)
        manifest["tar_sha256"] = "0" * 64  # content binding will fail
        row = _mount_row(
            staged_summary={"signature": "sig", "tar_sha256": "irrelevant"}
        )
        svc = _snapshot_service(
            {
                f"cloud-staging/{THREAD_ID}/manifest.json": json.dumps(
                    manifest
                ).encode(),
                f"cloud-staging/{THREAD_ID}/upper.tar": tar_bytes,
            }
        )
        with (
            _patch_endpoint(
                user=user,
                thread=thread,
                ro_mount_row=row,
                thread_mounts=[_thread_mounts_row()],
                snapshot_service=svc,
            ) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.get_thread_cloud_diff_file(
                THREAD_ID, "mod.txt", fake_request, dependencies=wired.dependencies
            )
        assert ei.value.status_code == 404
        assert ei.value.detail["code"] == "staged_content_unreadable"


# --------------------------------------------------------------------------- #
# POST /api/agents/threads/{thread_id}/cloud-diff/restage
# --------------------------------------------------------------------------- #


class TestCloudDiffRestage:
    @pytest.mark.asyncio
    async def test_restage_schedules_stage(self, fake_request):
        user = _make_user()
        thread = _make_thread(workspace=True)
        row = _mount_row(staged_summary=None)
        stage_mock = AsyncMock(return_value={"epoch": 1, "counts": {}})
        # ``_cloud_stage_tasks`` is now the ``stage`` half of the
        # application-owned ``CloudTaskRegistry``; the property is the live
        # dict, so seeding and eviction are observed exactly as before.
        stage_tasks = (
            orchestrator.main.app.state.resources.cloud_task_registry.cloud_stage_tasks
        )
        stage_tasks.clear()
        with (
            _patch_endpoint(
                user=user,
                thread=thread,
                ro_mount_row=row,
            ) as wired,
            patch(
                "orchestrator.services.cloud_staging.stage.stage_thread_cloud_diff",
                stage_mock,
            ),
        ):
            result = await diff_routes.restage_thread_cloud_diff(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )
            assert result == {"scheduled": True}
            assert len(stage_tasks) == 1
            task = next(iter(stage_tasks.values()))
            await task

            # Assert inside the patch context: main.postgres_db/snapshot_service
            # are only the patched fakes while the stack is still active.
            call = stage_mock.await_args
            assert call.kwargs["thread_id"] == THREAD_ID
            assert (
                call.kwargs["postgres_db"]
                is orchestrator.main.app.state.resources.postgres_db
            )
            assert (
                call.kwargs["snapshot_service"]
                is snapshot_service_module.snapshot_service
            )
            assert call.kwargs["authority"]["source_binding_sha256"] == _SOURCE.sha256
        assert not stage_tasks

    @pytest.mark.asyncio
    async def test_restage_409_without_workspace(self, fake_request):
        user = _make_user()
        thread = _make_thread(workspace=False)
        stage_tasks = (
            orchestrator.main.app.state.resources.cloud_task_registry.cloud_stage_tasks
        )
        stage_tasks.clear()
        with (
            _patch_endpoint(user=user, thread=thread) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.restage_thread_cloud_diff(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )
        assert ei.value.status_code == 409
        assert ei.value.detail == {"code": "no_workspace"}
        assert THREAD_ID not in stage_tasks


# --------------------------------------------------------------------------- #
# Auth propagation (shared by all three endpoints)
# --------------------------------------------------------------------------- #


class TestOwnerAuthPropagation:
    @pytest.mark.asyncio
    async def test_owner_auth_denied_for_other_user(self, fake_request):
        denied = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="Not your thread")
        )
        with (
            _patch_endpoint(
                user=_make_user(),
                thread=_make_thread(),
                require_thread_owner_result=denied,
            ) as wired,
            pytest.raises(HTTPException) as ei,
        ):
            await diff_routes.get_thread_cloud_diff_summary(
                THREAD_ID, fake_request, dependencies=wired.dependencies
            )
        assert ei.value.status_code == 403
        denied.assert_awaited_once()
