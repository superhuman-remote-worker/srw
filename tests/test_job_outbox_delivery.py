"""The job outbox's completion copy (S16b, main cloud slice 6.2).

``run_job_outbox_effect`` copies a root job's deliverables into its owner's
shared folder, records what happened in ``context.job_outbox`` and never
waits on Nextcloud's share bucket; the completion path that calls it is
pinned in ``tests/test_completion_class_a.py`` (``TestJobOutboxDelivery``).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import job_export
from orchestrator.services.cloud import SessionFolderHandle
from orchestrator.services.cloud.retry import LeakyBucket

JOB_ID = "682baab8-7864-4f3b-9298-4134294f703e"
OWNER = {
    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "keycloak_sub": "sub-1",
    "email": "owner@example.test",
}
FOLDER = SessionFolderHandle(backend="nextcloud", native_id="sessions/report-682baab8")


def _job(**over):
    job = {
        "id": JOB_ID,
        "status": "completed",
        "user_id": OWNER["id"],
        "description": "Write the report",
        "freeze_data": {"deliverables": ["output/report.md"]},
        "exported_folder_handle": None,
        "context": {},
    }
    job.update(over)
    return job


def _backend(**over):
    backend = MagicMock()
    backend.backend_id = "nextcloud"
    backend.is_initialized = True
    backend.ensure_session_folder = AsyncMock(return_value=FOLDER)
    backend.ensure_user = AsyncMock(return_value="owner")
    backend.share_session_folder = AsyncMock()
    backend.put_session_file = AsyncMock()
    backend.get_session_folder_browser_url = MagicMock(return_value="https://c/f")
    backend.get_session_folder_webdav_url = MagicMock(return_value="https://c/dav")
    for key, value in over.items():
        setattr(backend, key, value)
    return backend


def _deps(*, job=None, owner=OWNER, backend=None):
    store = SimpleNamespace(
        get_job=AsyncMock(return_value=job if job is not None else _job()),
        get_user=AsyncMock(return_value=owner),
        update_job_exported_folder=AsyncMock(return_value=True),
        merge_job_context=AsyncMock(return_value=True),
    )
    forge = SimpleNamespace(
        is_initialized=True,
        list_contents=AsyncMock(return_value=[]),
        get_file_bytes=AsyncMock(return_value=b"# report"),
    )
    backend = backend if backend is not None else _backend()
    return (
        job_export.JobExportDependencies(
            store=store,
            forge=forge,
            cloud_router=SimpleNamespace(for_owner=lambda _user: backend),
            resolve_job_repo=AsyncMock(return_value=("job-682baab8", None)),
        ),
        store,
        backend,
    )


def _record(store):
    return store.merge_job_context.await_args.args[1]["job_outbox"]


@pytest.mark.asyncio
async def test_the_deliverables_land_shared_and_the_job_is_stamped():
    deps, store, backend = _deps()

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert result == {
        "state": "delivered",
        "files": 1,
        "shared": True,
        "unshared": None,
    }
    assert backend.put_session_file.await_args.kwargs["path"] == "report.md"
    store.update_job_exported_folder.assert_awaited_once_with(
        JOB_ID, handle=FOLDER.to_db()
    )
    record = _record(store)
    assert {key: record[key] for key in ("state", "trigger", "files", "shared")} == {
        "state": "delivered",
        "trigger": "completion",
        "files": 1,
        "shared": True,
    }
    assert record["handle"] == FOLDER.to_db()


@pytest.mark.asyncio
async def test_a_full_share_bucket_never_blocks_the_completion():
    # The real bucket, full: bounded_wait(0) turns its wait into BucketFull.
    bucket = LeakyBucket(max_events=1, window_seconds=600)
    await bucket.acquire()

    async def share(_handle, _user):
        await bucket.acquire()

    deps, store, _backend_ = _deps(backend=_backend(share_session_folder=share))

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert (result["state"], result["shared"], result["unshared"]) == (
        "delivered",
        False,
        "rate_limited",
    )
    assert _record(store)["unshared"] == "rate_limited"


@pytest.mark.asyncio
async def test_a_failed_copy_is_recorded_with_its_reason():
    from orchestrator.services.cloud import CloudBackendError, CloudBackendErrorKind

    backend = _backend(
        ensure_session_folder=AsyncMock(
            side_effect=CloudBackendError(
                CloudBackendErrorKind.UNKNOWN, "nope", backend="nextcloud"
            )
        )
    )
    deps, store, _ = _deps(backend=backend)

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert (result["state"], result["reason"]) == ("failed", "folder_failed")
    assert _record(store)["reason"] == "folder_failed"
    store.update_job_exported_folder.assert_not_awaited()


@pytest.mark.asyncio
async def test_without_an_owner_nothing_is_copied():
    deps, store, backend = _deps(owner=None)

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert result == {"state": "failed", "reason": "owner_unavailable"}
    backend.ensure_session_folder.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "recorded",
    [
        {"shared_with": OWNER["id"]},
        # Recorded before the recipient was: a completion shared with the owner.
        {"trigger": "completion"},
    ],
    ids=["shared-with-the-owner", "an-older-completion-record"],
)
async def test_a_second_completion_reuses_the_folder_and_the_owners_share(recorded):
    handle = FOLDER.to_db()
    job = _job(
        exported_folder_handle=handle,
        context={
            "job_outbox": {
                "state": "delivered",
                "shared": True,
                "handle": handle,
                **recorded,
            }
        },
    )
    deps, store, backend = _deps(job=job)

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert result["shared"] is True
    assert backend.ensure_session_folder.await_args.kwargs == {
        "session_id": "report-682baab8",
        "exclusive": False,
    }
    backend.share_session_folder.assert_not_awaited()
    assert _record(store)["shared_with"] == OWNER["id"]


@pytest.mark.asyncio
async def test_a_folder_shared_with_someone_else_is_shared_with_the_owner():
    """An older re-sync shared the folder with the member who asked; the
    owner's completion copy shares it with the owner."""
    handle = FOLDER.to_db()
    job = _job(
        exported_folder_handle=handle,
        context={
            "job_outbox": {
                "state": "delivered",
                "trigger": "resync",
                "shared": True,
                "shared_with": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "handle": handle,
            }
        },
    )
    deps, store, backend = _deps(job=job)

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert result["shared"] is True
    backend.share_session_folder.assert_awaited_once_with(FOLDER, "owner")
    assert _record(store)["shared_with"] == OWNER["id"]


@pytest.mark.asyncio
async def test_a_new_folder_is_the_jobs_own_before_the_first_file():
    from orchestrator.services.cloud import CloudBackendError, CloudBackendErrorKind

    backend = _backend(
        put_session_file=AsyncMock(
            side_effect=CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE, "gone", backend="nextcloud"
            )
        )
    )
    deps, store, _ = _deps(backend=backend)

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert (result["state"], result["reason"]) == ("failed", "copy_failed")
    assert backend.ensure_session_folder.await_args.kwargs == {
        "session_id": "write-the-report-" + JOB_ID.replace("-", ""),
        "exclusive": True,
    }
    copying, failed = [
        call.args[1]["job_outbox"] for call in store.merge_job_context.await_args_list
    ]
    assert (copying["state"], copying["handle"]) == ("copying", FOLDER.to_db())
    # The failure keeps it: the next copy resumes into the job's own folder.
    assert failed["handle"] == FOLDER.to_db()
    store.update_job_exported_folder.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_main_cloud_for_the_owner_is_a_recorded_refusal():
    deps, store, _ = _deps()

    def refuse(_owner):
        raise RuntimeError("no durable active backend-instance authority")

    deps = job_export.JobExportDependencies(
        store=deps.store,
        forge=deps.forge,
        cloud_router=SimpleNamespace(for_owner=refuse),
        resolve_job_repo=deps.resolve_job_repo,
    )

    result = await job_export.run_job_outbox_effect(job_id=JOB_ID, dependencies=deps)

    assert (result["state"], result["reason"]) == ("failed", "backend_unavailable")
    assert _record(store)["reason"] == "backend_unavailable"
