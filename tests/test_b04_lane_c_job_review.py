"""R1.B04 lane C — Mode B export, live PR status and the job review session.

Characterization first: ``tests/test_export_to_cloud_endpoint.py``,
``tests/test_job_pull_request_status.py`` and ``tests/test_job_review_session.py``
already drive these three handlers through ``orchestrator.main``. This suite
re-states the properties that a *handler move* can quietly change and that the
existing suites do not pin: which gate guards which route (and that the export
alone needs the authenticated user, not just the row), that the deterministic
folder name and the collapsed prefix survive, that a failed share is a success
with ``shared: false``, and that the review session's server-authored seed is
still set on a private attribute JSON cannot reach.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers.job_review import (
    JobReviewDependencies,
    create_job_review_session,
    export_job_to_shared_folder,
    get_job_pull_request_status,
    router as job_review_router,
)
from orchestrator.services import job_export, job_review_session
from orchestrator.services.cloud import CloudBackendError, SessionFolderHandle
from orchestrator.services.cloud.errors import CloudBackendErrorKind
from tests._mounted_router import mount_router
from tests._route_inventory import mounted_route_objects


JOB_ID = "a6fa6f2a-9101-4c1e-9b9e-0000000000c1"
PROJECT_ID = "b6fa6f2a-9101-4c1e-9b9e-0000000000c2"
DATASOURCE_ID = "c6fa6f2a-9101-4c1e-9b9e-0000000000c3"
USER = {
    "id": "d6fa6f2a-9101-4c1e-9b9e-0000000000c4",
    "email": "dev@example.test",
    "keycloak_sub": "sub-1",
    "display_name": "Dev",
    "preferred_username": "dev",
}
REPO = "job-a6fa6f2a"
BRANCH = "job/a6fa6f2a"


# =============================================================================
# Pure helpers: the deterministic folder name and the collapsed prefix
# =============================================================================


class TestJobExportFolderName:
    def test_the_slug_leads_and_the_short_id_disambiguates(self):
        assert (
            job_export._job_export_folder_name(JOB_ID, "You maintain a daily digest")
            == "you-maintain-a-daily-digest-a6fa6f2a"
        )

    def test_the_same_job_always_maps_to_the_same_name(self):
        first = job_export._job_export_folder_name(JOB_ID, "Ship it")
        second = job_export._job_export_folder_name(JOB_ID, "Ship it")

        assert first == second == "ship-it-a6fa6f2a"

    def test_accents_are_folded_rather_than_dropped_to_dashes(self):
        assert job_export._job_export_folder_name(JOB_ID, "Führe aus").startswith(
            "fuhre-aus-"
        )

    def test_only_the_first_line_of_a_multi_line_description_is_used(self):
        assert job_export._job_export_folder_name(
            JOB_ID, "First line\nSecond line"
        ) == ("first-line-a6fa6f2a")

    def test_a_long_description_is_cut_on_a_word_boundary(self):
        name = job_export._job_export_folder_name(
            JOB_ID, "alpha bravo charlie delta echo foxtrot golf hotel india"
        )

        assert name.endswith("-a6fa6f2a")
        slug = name[: -len("-a6fa6f2a")]
        assert len(slug) <= job_export._EXPORT_SLUG_MAX
        assert not slug.endswith("-")

    @pytest.mark.parametrize("description", ["", None, "   ", "!!! ???"])
    def test_an_unusable_description_falls_back_to_the_job_id(self, description):
        assert job_export._job_export_folder_name(JOB_ID, description) == (
            "job-a6fa6f2a9101"
        )

    def test_the_name_is_a_single_safe_path_segment(self):
        name = job_export._job_export_folder_name(JOB_ID, "a/b\\c ../d")

        assert "/" not in name and "\\" not in name and ".." not in name


class TestCommonDirPrefix:
    @pytest.mark.parametrize(
        ("paths", "expected"),
        [
            ([], ""),
            (["output/digest.md"], "output"),
            (["done.txt"], ""),
            (["repo/src/a.py", "repo/tests/b.py"], "repo"),
            (["spec.yaml", "repo/a.py"], ""),
            (["out/deep/a.md", "out/deep/b.md"], "out/deep"),
            (["out/a.md", "output/b.md"], ""),
        ],
    )
    def test_whole_segments_only(self, paths, expected):
        assert job_export._common_dir_prefix(paths) == expected


# =============================================================================
# POST /api/jobs/{job_id}/export-to-shared-folder
# =============================================================================


def _job(**over):
    job = {
        "id": JOB_ID,
        "status": "completed",
        "description": "Ship it",
        "project_has_cloud_folder": False,
        "freeze_data": {"deliverables": ["output/digest.md"]},
        "context": {},
    }
    job.update(over)
    return job


def _backend(**over):
    backend = MagicMock()
    backend.backend_id = "nextcloud"
    backend.is_initialized = True
    backend.ensure_session_folder = AsyncMock(
        return_value=SessionFolderHandle(backend="nextcloud", native_id="/folder/17")
    )
    backend.ensure_user = AsyncMock(return_value="cloud-user-1")
    backend.share_session_folder = AsyncMock()
    backend.put_session_file = AsyncMock()
    backend.get_session_folder_browser_url = MagicMock(return_value="https://c/f")
    backend.get_session_folder_webdav_url = MagicMock(return_value="https://c/dav")
    for key, value in over.items():
        setattr(backend, key, value)
    return backend


def _export_deps(*, job=None, backend=None, forge=None, store=None, gate=None):
    backend = backend if backend is not None else _backend()
    forge = forge or SimpleNamespace(
        is_initialized=True,
        list_contents=AsyncMock(return_value=[]),
        get_file_bytes=AsyncMock(return_value=b"payload"),
    )
    store = store or SimpleNamespace(update_job_exported_folder=AsyncMock())
    export = job_export.JobExportDependencies(
        store=store,
        forge=forge,
        cloud_router=SimpleNamespace(for_owner=lambda _user: backend),
        resolve_job_repo=AsyncMock(return_value=(REPO, BRANCH)),
    )
    review = job_review_session.JobReviewSessionDependencies(
        store=store,
        create_thread=AsyncMock(),
        thread_create_request=MagicMock(),
        trusted_thread_seed=MagicMock(),
        bundled_expert_bundle=MagicMock(return_value=None),
    )
    return JobReviewDependencies(
        store=SimpleNamespace(name="auth-store"),
        export=export,
        review_session=review,
        require_job_access=gate
        or AsyncMock(return_value=(USER, job if job is not None else _job())),
    )


def _request():
    return SimpleNamespace(headers={})


class TestExportToSharedFolder:
    @pytest.mark.asyncio
    async def test_the_gate_supplies_both_the_user_and_the_row(self):
        """Export is the one lane C route that needs the caller, not just the
        row: the cloud folder is provisioned and shared in that user's name."""
        gate = AsyncMock(return_value=(USER, _job()))
        backend = _backend()
        deps = _export_deps(gate=gate, backend=backend)
        seen = {}

        def for_owner(user):
            seen["user"] = user
            return backend

        deps.export.cloud_router.for_owner = for_owner

        await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        gate.assert_awaited_once()
        assert seen["user"] is USER
        assert backend.ensure_user.await_args.kwargs["sub"] == "sub-1"
        assert backend.ensure_user.await_args.kwargs["email"] == USER["email"]

    @pytest.mark.parametrize("status", ["processing", "failed", "created", "paused"])
    @pytest.mark.asyncio
    async def test_only_completed_or_in_review_jobs_export(self, status):
        deps = _export_deps(job=_job(status=status))

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            f"Job is in status '{status}'; "
            "only completed or in-review jobs can be exported."
        )

    @pytest.mark.asyncio
    async def test_a_cloud_folder_project_resyncs_its_outbox_too(self):
        deps = _export_deps(job=_job(project_has_cloud_folder=True))

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["files_copied"] == 1
        assert result["shared"] is True

    @pytest.mark.asyncio
    async def test_a_job_in_review_is_exported(self):
        deps = _export_deps(job=_job(status="reviewing"))
        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )
        assert result["files_copied"] == 1

    @pytest.mark.asyncio
    async def test_a_full_share_bucket_leaves_the_copy_unshared(self):
        from orchestrator.services.cloud.retry import BucketFull

        backend = _backend(share_session_folder=AsyncMock(side_effect=BucketFull(90)))
        deps = _export_deps(backend=backend)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert (result["shared"], result["unshared"]) == (False, "rate_limited")
        assert result["files_copied"] == 1

    @pytest.mark.asyncio
    async def test_a_share_error_after_the_copy_is_unshared_not_a_502(self):
        backend = _backend(
            share_session_folder=AsyncMock(side_effect=_cloud_error("dup"))
        )
        deps = _export_deps(backend=backend)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert (result["shared"], result["unshared"]) == (False, "share_failed")

    @pytest.mark.asyncio
    async def test_a_folder_an_earlier_copy_shared_is_not_shared_again(self):
        handle = SessionFolderHandle(backend="nextcloud", native_id="/folder/17")
        backend = _backend()
        job = _job(
            exported_folder_handle=handle.to_db(),
            context={"job_outbox": {"shared": True, "handle": handle.to_db()}},
        )
        deps = _export_deps(job=job, backend=backend)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["shared"] is True
        backend.share_session_folder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_uninitialized_backend_refuses_before_the_forge_check(self):
        deps = _export_deps(
            backend=_backend(is_initialized=False),
            forge=SimpleNamespace(is_initialized=False),
        )

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        assert exc.value.detail == "Cloud backend not available."

    @pytest.mark.asyncio
    async def test_an_offline_forge_refuses_before_the_repo_is_resolved(self):
        deps = _export_deps(
            forge=SimpleNamespace(
                is_initialized=False,
                get_file_bytes=AsyncMock(),
                list_contents=AsyncMock(),
            )
        )

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        assert exc.value.detail == "Gitea not available."
        deps.export.resolve_job_repo.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_previously_exported_job_reuses_its_stored_folder(self):
        handle = SessionFolderHandle(backend="nextcloud", native_id="/root/old-name")
        deps = _export_deps(job=_job(exported_folder_handle=handle.to_db()))

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        deps.export.cloud_router.for_owner(
            USER
        ).ensure_session_folder.assert_awaited_once_with(session_id="old-name")
        assert result["folder"]["name"] == "old-name"

    @pytest.mark.asyncio
    async def test_a_fresh_export_derives_the_name_from_the_job(self):
        deps = _export_deps()

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["folder"]["name"] == "ship-it-a6fa6f2a"
        assert result["folder"]["path"] == "/ship-it-a6fa6f2a"

    @pytest.mark.asyncio
    async def test_the_common_prefix_is_collapsed_on_the_destination(self):
        backend = _backend()
        deps = _export_deps(
            job=_job(freeze_data={"deliverables": ["output/digest.md"]}),
            backend=backend,
        )

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        backend.put_session_file.assert_awaited_once()
        assert backend.put_session_file.await_args.kwargs["path"] == "digest.md"
        assert result["files_copied"] == 1

    @pytest.mark.asyncio
    async def test_distinguishing_structure_survives_the_collapse(self):
        backend = _backend()
        deps = _export_deps(
            job=_job(
                freeze_data={"deliverables": ["repo/src/a.py", "repo/tests/b.py"]}
            ),
            backend=backend,
        )

        await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert [
            call.kwargs["path"] for call in backend.put_session_file.await_args_list
        ] == ["src/a.py", "tests/b.py"]

    @pytest.mark.asyncio
    async def test_an_unsafe_declared_path_is_skipped_not_copied(self):
        backend = _backend()
        deps = _export_deps(
            job=_job(
                freeze_data={"deliverables": ["../etc/passwd", "/", "output/digest.md"]}
            ),
            backend=backend,
        )

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["files_copied"] == 1
        assert backend.put_session_file.await_args.kwargs["path"] == "digest.md"

    @pytest.mark.asyncio
    async def test_a_declared_but_missing_deliverable_is_fail_soft(self):
        forge = SimpleNamespace(
            is_initialized=True,
            list_contents=AsyncMock(return_value=[]),
            get_file_bytes=AsyncMock(side_effect=[None, b"ok"]),
        )
        deps = _export_deps(
            job=_job(freeze_data={"deliverables": ["out/gone.md", "out/here.md"]}),
            forge=forge,
        )

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["files_copied"] == 1

    @pytest.mark.asyncio
    async def test_the_output_fallback_is_required_and_502s_on_a_missing_file(self):
        forge = SimpleNamespace(
            is_initialized=True,
            list_contents=AsyncMock(
                return_value=[{"type": "file", "path": "output/a.md"}]
            ),
            get_file_bytes=AsyncMock(return_value=None),
        )
        deps = _export_deps(job=_job(freeze_data=None), forge=forge)

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == "Failed to read 'output/a.md' from Gitea."

    @pytest.mark.asyncio
    async def test_the_output_fallback_walks_directories_recursively(self):
        async def list_contents(_repo, path, *, ref=None):
            if path == "output":
                return [
                    {"type": "dir", "path": "output/sub"},
                    {"type": "file", "path": "output/a.md"},
                ]
            return [{"type": "file", "path": "output/sub/b.md"}]

        forge = SimpleNamespace(
            is_initialized=True,
            list_contents=AsyncMock(side_effect=list_contents),
            get_file_bytes=AsyncMock(return_value=b"x"),
        )
        backend = _backend()
        deps = _export_deps(job=_job(freeze_data=None), forge=forge, backend=backend)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["files_copied"] == 2
        assert sorted(
            call.kwargs["path"] for call in backend.put_session_file.await_args_list
        ) == ["a.md", "sub/b.md"]

    @pytest.mark.asyncio
    async def test_freeze_data_arriving_as_a_json_string_is_parsed(self):
        backend = _backend()
        deps = _export_deps(
            job=_job(freeze_data='{"deliverables": ["output/digest.md"]}'),
            backend=backend,
        )

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["files_copied"] == 1
        assert backend.put_session_file.await_args.kwargs["path"] == "digest.md"

    @pytest.mark.asyncio
    async def test_an_unshareable_folder_is_a_success_marked_unshared(self):
        backend = _backend(ensure_user=AsyncMock(return_value=None))
        deps = _export_deps(backend=backend)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        assert result["shared"] is False
        backend.share_session_folder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_provisioning_failure_is_a_502(self):
        backend = _backend(
            ensure_session_folder=AsyncMock(side_effect=_cloud_error("nope"))
        )
        deps = _export_deps(backend=backend)

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == (
            "Cloud folder provisioning failed: [nextcloud:unknown] nope"
        )

    @pytest.mark.asyncio
    async def test_an_upload_failure_reports_what_actually_landed(self):
        backend = _backend(
            put_session_file=AsyncMock(side_effect=[None, _cloud_error("quota")])
        )
        deps = _export_deps(
            job=_job(freeze_data={"deliverables": ["out/a.md", "out/b.md"]}),
            backend=backend,
        )

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == (
            "File copy to cloud failed after 1 files: [nextcloud:unknown] quota"
        )

    @pytest.mark.asyncio
    async def test_an_unexpected_failure_is_a_502_not_a_500(self):
        backend = _backend(put_session_file=AsyncMock(side_effect=ValueError("odd")))
        deps = _export_deps(backend=backend)

        with pytest.raises(HTTPException) as exc:
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == "Export failed: odd"

    @pytest.mark.asyncio
    async def test_the_job_is_stamped_only_after_a_successful_copy(self):
        store = SimpleNamespace(update_job_exported_folder=AsyncMock())
        backend = _backend(put_session_file=AsyncMock(side_effect=ValueError("odd")))
        deps = _export_deps(backend=backend, store=store)

        with pytest.raises(HTTPException):
            await export_job_to_shared_folder(_request(), JOB_ID, dependencies=deps)

        store.update_job_exported_folder.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_successful_export_stamps_the_handle(self):
        store = SimpleNamespace(update_job_exported_folder=AsyncMock())
        deps = _export_deps(store=store)

        result = await export_job_to_shared_folder(
            _request(), JOB_ID, dependencies=deps
        )

        store.update_job_exported_folder.assert_awaited_once()
        assert store.update_job_exported_folder.await_args.args == (JOB_ID,)
        assert result["folder"]["browser_url"] == "https://c/f"
        assert result["folder"]["webdav_url"] == "https://c/dav"
        assert result["shared"] is True


def _cloud_error(message: str) -> CloudBackendError:
    return CloudBackendError(
        CloudBackendErrorKind.UNKNOWN, message, backend="nextcloud"
    )


# =============================================================================
# GET /api/jobs/{job_id}/pull-request
# =============================================================================


def _pull_request(**over):
    values = {
        "forge": "gitea",
        "repo": "org/app",
        "number": 7,
        "url": "https://forge/org/app/pulls/7",
        "head": "srw/job",
        "base": "main",
    }
    values.update(over)
    return SimpleNamespace(**values)


def _review_deps(*, job=None, store=None, create_thread=None, bundle=None):
    store = store or SimpleNamespace(
        resolve_datasources_for_job=AsyncMock(
            return_value=[
                {"id": DATASOURCE_ID, "connection_url": "https://forge/org/app.git"}
            ]
        )
    )
    review = job_review_session.JobReviewSessionDependencies(
        store=store,
        create_thread=create_thread
        or AsyncMock(return_value={"thread_id": "t-1", "status": "created"}),
        thread_create_request=_FakeThreadCreateRequest,
        trusted_thread_seed=_FakeTrustedSeed,
        bundled_expert_bundle=bundle or (lambda _name: None),
    )
    export = job_export.JobExportDependencies(
        store=store,
        forge=SimpleNamespace(is_initialized=True),
        cloud_router=SimpleNamespace(for_owner=lambda _u: _backend()),
        resolve_job_repo=AsyncMock(return_value=(REPO, BRANCH)),
    )
    return JobReviewDependencies(
        store=SimpleNamespace(name="auth-store"),
        export=export,
        review_session=review,
        require_job_access=AsyncMock(
            return_value=(USER, job if job is not None else _review_job())
        ),
    )


class _FakeThreadCreateRequest:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._trusted_seed = None


class _FakeTrustedSeed:
    def __init__(self, *, metadata, opening_event):
        self.metadata = metadata
        self.opening_event = opening_event


def _review_job(**over):
    job = {
        "id": JOB_ID,
        "description": "Fix the thing",
        "project_id": PROJECT_ID,
        "config_name": "developer",
        "context": {},
        "config_override": {},
        "resolved_config": {},
        "freeze_data": {},
    }
    job.update(over)
    return job


class TestPullRequestStatus:
    @pytest.mark.asyncio
    async def test_a_job_without_a_recorded_pr_is_a_404(self):
        deps = _review_deps()

        with patch(
            "orchestrator.services.job_delivery.parse_job_pull_request",
            MagicMock(return_value=None),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == "This job has no recorded pull request"

    @pytest.mark.asyncio
    async def test_a_connector_resolution_failure_never_leaks_the_cause(self):
        store = SimpleNamespace(
            resolve_datasources_for_job=AsyncMock(
                side_effect=RuntimeError("password=hunter2")
            )
        )
        deps = _review_deps(store=store)

        with patch(
            "orchestrator.services.job_delivery.parse_job_pull_request",
            MagicMock(return_value=_pull_request()),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 500
        assert exc.value.detail == "Could not resolve this job's repository"

    @pytest.mark.asyncio
    async def test_a_detached_repository_is_a_409(self):
        deps = _review_deps()

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(return_value=None),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "The recorded pull request's repository is no longer attached"
        )

    @pytest.mark.asyncio
    async def test_a_forge_error_is_a_502_that_names_nothing(self):
        from shared.runtime.services.forge import ForgeError

        deps = _review_deps()

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(return_value={"id": DATASOURCE_ID}),
            ),
            patch(
                "orchestrator.services.job_delivery.forge_repo_from_datasource",
                MagicMock(return_value=object()),
            ),
            patch(
                "shared.runtime.services.forge.get_pull_request_status",
                AsyncMock(side_effect=ForgeError("token=abc")),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await get_job_pull_request_status(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == "Live pull request status is unavailable"

    @pytest.mark.asyncio
    async def test_the_recorded_identity_backfills_display_fields(self):
        deps = _review_deps()

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(return_value={"id": DATASOURCE_ID}),
            ),
            patch(
                "orchestrator.services.job_delivery.forge_repo_from_datasource",
                MagicMock(return_value=object()),
            ),
            patch(
                "shared.runtime.services.forge.get_pull_request_status",
                AsyncMock(return_value={"state": "open", "url": None}),
            ),
        ):
            result = await get_job_pull_request_status(
                _request(), JOB_ID, dependencies=deps
            )

        assert result == {
            "forge": "gitea",
            "repo": "org/app",
            "state": "open",
            "url": "https://forge/org/app/pulls/7",
            "head": "srw/job",
            "base": "main",
        }


# =============================================================================
# Review-session config derivation
# =============================================================================


class TestReviewSessionConfigValues:
    def test_only_model_and_temperature_are_carried_over(self):
        job = _review_job(
            resolved_config={"agent": {"llm": {"model": " gpt-x ", "temperature": 1}}},
            config_override={"llm": {"model": "ignored", "top_p": 0.5}},
        )

        model, temperature, dropped = job_review_session._review_session_config_values(
            job
        )

        assert model == "gpt-x"
        assert temperature == 1.0
        assert dropped == ["llm.top_p"]

    def test_the_override_is_the_fallback_when_nothing_was_resolved(self):
        job = _review_job(config_override={"llm": {"model": "m", "temperature": 0.3}})

        model, temperature, dropped = job_review_session._review_session_config_values(
            job
        )

        assert (model, temperature, dropped) == ("m", 0.3, [])

    def test_a_boolean_is_not_a_temperature(self):
        job = _review_job(config_override={"llm": {"temperature": True}})

        _model, temperature, _dropped = (
            job_review_session._review_session_config_values(job)
        )

        assert temperature is None

    def test_unsupported_keys_are_reported_by_name_never_by_value(self):
        job = _review_job(
            config_override={
                "workspace": {"credentials": {"token": "hunter2"}},
                "interactive": {"voice": True},
                "llm": {"api_key": "sk-secret"},
            }
        )

        _model, _temperature, dropped = (
            job_review_session._review_session_config_values(job)
        )

        assert sorted(dropped) == ["interactive.voice", "llm.api_key", "workspace"]
        assert "hunter2" not in " ".join(dropped)
        assert "sk-secret" not in " ".join(dropped)


class TestReviewSessionConfigName:
    def test_a_session_base_job_keeps_its_profile(self):
        name = job_review_session._review_session_config_name(
            _review_job(config_name="session_base"),
            bundled_expert_bundle=lambda _n: None,
        )

        assert name == "session_base"

    def test_a_bundled_session_expert_is_kept(self):
        name = job_review_session._review_session_config_name(
            _review_job(config_name="assistant"),
            bundled_expert_bundle=lambda _n: {"expert_type": "session"},
        )

        assert name == "assistant"

    def test_a_worker_expert_falls_back_to_the_session_base(self):
        name = job_review_session._review_session_config_name(
            _review_job(config_name="developer"),
            bundled_expert_bundle=lambda _n: {"expert_type": "worker"},
        )

        assert name == "session_base"

    def test_an_unknown_profile_falls_back_to_the_session_base(self):
        name = job_review_session._review_session_config_name(
            _review_job(config_name="config/experts/custom.yaml"),
            bundled_expert_bundle=lambda _n: None,
        )

        assert name == "session_base"


class TestBriefText:
    def test_non_strings_are_dropped(self):
        assert job_review_session._brief_text(None, limit=10) == ""
        assert job_review_session._brief_text(17, limit=10) == ""

    def test_whitespace_is_collapsed_and_the_result_is_bounded(self):
        assert job_review_session._brief_text("a\n\n  b\tc", limit=100) == "a b c"
        assert job_review_session._brief_text("x" * 50, limit=10) == "x" * 10


class TestReviewSessionOpeningEvent:
    def test_the_event_is_server_labelled_and_states_the_delivery(self):
        event = job_review_session._review_session_opening_event(
            _review_job(),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=[],
        )

        assert event.startswith("[Server-derived job review context]")
        assert f"Job: {JOB_ID}" in event
        assert "Source repository: org/app" in event
        assert "Delivered branch: srw/job" in event
        assert "Pull request: #7 (https://forge/org/app/pulls/7)" in event
        assert "Permission mode: supervised for review" in event

    def test_a_profile_change_is_explained(self):
        event = job_review_session._review_session_opening_event(
            _review_job(config_name="developer"),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=[],
        )

        assert "worker and session " in event
        assert "profiles have different schemas." in event

    def test_dropped_settings_are_listed_by_name(self):
        event = job_review_session._review_session_opening_event(
            _review_job(),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=["workspace", "llm.api_key"],
        )

        assert "Job-only settings not inherited: workspace, llm.api_key." in event

    def test_deliverables_come_from_the_context_then_the_freeze_data(self):
        from_context = job_review_session._review_session_opening_event(
            _review_job(context={"required_deliverables": ["a.md"]}),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=[],
        )
        from_freeze = job_review_session._review_session_opening_event(
            _review_job(freeze_data={"deliverables": ["b.md"]}),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=[],
        )

        assert "- a.md" in from_context
        assert "- b.md" in from_freeze

    def test_a_huge_event_is_truncated_with_an_ellipsis(self):
        event = job_review_session._review_session_opening_event(
            _review_job(description="x " * 20_000),
            pull_request=_pull_request(),
            session_config_name="session_base",
            dropped_settings=["k" * 200] * 100,
        )

        assert len(event) <= 19_500
        assert event.endswith("…")


# =============================================================================
# POST /api/jobs/{job_id}/review-session
# =============================================================================


class TestCreateJobReviewSession:
    @pytest.mark.asyncio
    async def test_a_job_without_a_delivered_branch_is_a_409(self):
        deps = _review_deps()

        with patch(
            "orchestrator.services.job_delivery.parse_job_pull_request",
            MagicMock(return_value=None),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "This job has no recorded delivered branch to review"
        )

    @pytest.mark.asyncio
    async def test_a_connector_resolution_failure_is_an_opaque_500(self):
        store = SimpleNamespace(
            resolve_datasources_for_job=AsyncMock(side_effect=RuntimeError("boom"))
        )
        deps = _review_deps(store=store)

        with patch(
            "orchestrator.services.job_delivery.parse_job_pull_request",
            MagicMock(return_value=_pull_request()),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 500
        assert exc.value.detail == "Could not resolve this job's connectors"

    @pytest.mark.asyncio
    async def test_a_detached_delivery_repository_is_a_409(self):
        deps = _review_deps()

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(return_value=None),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "The delivered repository is no longer attached to this job"
        )

    @pytest.mark.asyncio
    async def test_an_id_less_connector_anywhere_in_the_set_refuses(self):
        store = SimpleNamespace(
            resolve_datasources_for_job=AsyncMock(
                return_value=[
                    {"id": DATASOURCE_ID, "connection_url": "https://forge/a.git"},
                    {"connection_url": "https://forge/b.git"},
                ]
            )
        )
        deps = _review_deps(store=store)

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(
                    return_value={
                        "id": DATASOURCE_ID,
                        "connection_url": "https://forge/a.git",
                    }
                ),
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await create_job_review_session(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 500
        assert exc.value.detail == "Could not resolve this job's connectors"

    @pytest.mark.asyncio
    async def test_the_created_thread_is_supervised_sandbox_and_server_seeded(self):
        created = AsyncMock(return_value={"thread_id": "t-1", "status": "created"})
        deps = _review_deps(create_thread=created)
        request = _request()

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(
                    return_value={
                        "id": DATASOURCE_ID,
                        "connection_url": "https://forge/org/app.git",
                    }
                ),
            ),
            patch(
                "orchestrator.services.job_delivery.repository_host",
                MagicMock(return_value="forge"),
            ),
        ):
            result = await create_job_review_session(request, JOB_ID, dependencies=deps)

        body, passed_request = created.await_args.args
        assert passed_request is request
        assert body.kwargs["permission_mode"] == "supervised"
        assert body.kwargs["config_override"] == {"workspace": {"backend": "sandbox"}}
        assert body.kwargs["config_name"] == "session_base"
        assert body.kwargs["project_ids"] == [PROJECT_ID]
        assert body.kwargs["datasource_ids"] == [DATASOURCE_ID]
        assert body.kwargs["title"] == "Review job a6fa6f2a: Fix the thing"
        # The seed is a *private* attribute — JSON can never populate it.
        assert body._trusted_seed.metadata["review_delivery"] == {
            "job_id": JOB_ID,
            "datasource_id": DATASOURCE_ID,
            "forge": "gitea",
            "repository_host": "forge",
            "repo": "org/app",
            "branch": "srw/job",
            "base": "main",
            "pull_request": {
                "number": 7,
                "url": "https://forge/org/app/pulls/7",
            },
        }
        assert body._trusted_seed.opening_event.startswith(
            "[Server-derived job review context]"
        )
        assert result == {"job_id": JOB_ID, "thread_id": "t-1", "status": "created"}

    @pytest.mark.asyncio
    async def test_a_loose_job_scopes_the_thread_to_no_project(self):
        created = AsyncMock(return_value={"thread_id": "t-1"})
        deps = _review_deps(job=_review_job(project_id=None), create_thread=created)

        with (
            patch(
                "orchestrator.services.job_delivery.parse_job_pull_request",
                MagicMock(return_value=_pull_request()),
            ),
            patch(
                "orchestrator.services.job_delivery.find_pull_request_repository",
                MagicMock(
                    return_value={
                        "id": DATASOURCE_ID,
                        "connection_url": "https://forge/org/app.git",
                    }
                ),
            ),
            patch(
                "orchestrator.services.job_delivery.repository_host",
                MagicMock(return_value="forge"),
            ),
        ):
            result = await create_job_review_session(
                _request(), JOB_ID, dependencies=deps
            )

        body, _request_arg = created.await_args.args
        assert body.kwargs["project_ids"] is None
        # The default status is applied when the creator does not report one.
        assert result["status"] == "created"


# =============================================================================
# Wire: paths, gates and per-invocation dependency resolution
# =============================================================================


class TestJobReviewWire:
    def test_routes_keep_their_paths_and_methods(self):
        app = mount_router(job_review_router)
        # ``app.routes`` is not the API on FastAPI >= 0.139: an include is one
        # ``_IncludedRouter`` wrapper with no path or methods of its own, so the
        # obvious comprehension silently sees zero of these routes. The pin is
        # ``fastapi>=0.109.0``, so a fresh install resolves the new shape while a
        # long-lived venv sits on the old one — read it through the helper.
        declared = {
            (route.path, tuple(sorted(route.methods)))
            for route in mounted_route_objects(app)
        }

        assert (
            "/api/jobs/{job_id}/export-to-shared-folder",
            ("POST",),
        ) in declared
        assert ("/api/jobs/{job_id}/pull-request", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/review-session", ("POST",)) in declared

    def test_every_route_is_guarded_by_require_job_access(self):
        deps = _export_deps(
            gate=AsyncMock(side_effect=HTTPException(403, "Access denied"))
        )
        app = mount_router(
            job_review_router,
            factories={"job_review_dependencies_factory": lambda: deps},
        )

        with TestClient(app) as client:
            assert (
                client.post(f"/api/jobs/{JOB_ID}/export-to-shared-folder").status_code
                == 403
            )
            assert client.get(f"/api/jobs/{JOB_ID}/pull-request").status_code == 403
            assert client.post(f"/api/jobs/{JOB_ID}/review-session").status_code == 403

    def test_the_review_session_route_accepts_no_request_body(self):
        deps = _review_deps()
        app = mount_router(
            job_review_router,
            factories={"job_review_dependencies_factory": lambda: deps},
        )

        with patch(
            "orchestrator.services.job_delivery.parse_job_pull_request",
            MagicMock(return_value=None),
        ):
            with TestClient(app) as client:
                response = client.post(
                    f"/api/jobs/{JOB_ID}/review-session",
                    json={"config_override": {"workspace": {"backend": "vm"}}},
                )

        # The body is ignored, not validated: the handler still refuses on the
        # missing pull request rather than on the payload.
        assert response.status_code == 409

    def test_a_rebound_backend_is_observed_by_the_next_dependency_build(self):
        first = _export_deps()
        second = _export_deps(backend=_backend(is_initialized=False))
        holder = {"deps": first}
        app = mount_router(
            job_review_router,
            factories={"job_review_dependencies_factory": lambda: holder["deps"]},
        )

        with TestClient(app) as client:
            assert (
                client.post(f"/api/jobs/{JOB_ID}/export-to-shared-folder").status_code
                == 200
            )
            holder["deps"] = second
            response = client.post(f"/api/jobs/{JOB_ID}/export-to-shared-folder")

        assert response.status_code == 503
        assert response.json() == {"detail": "Cloud backend not available."}
