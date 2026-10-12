"""Characterization tests for the Mode A job diff endpoints (pre-DiffSource refactor).

Pins the CURRENT behavior of ``GET /api/jobs/{job_id}/diff`` and
``GET /api/jobs/{job_id}/diff/{file_path:path}`` (orchestrator/main.py) before
Task 6 extracts the ``DiffSource`` seam (services/diff_source.py). These two
endpoints had zero existing test coverage (confirmed by exploration
2026-07-12) — this file is the safety net the refactor runs against. Every
test here must stay green, unmodified, on both sides of the extraction: the
data-fetch moves into ``GiteaDiffSource``, but the endpoints' gates (404 no
baseline, 400 outside ``projects/``, 503 gitea uninitialized, 404 no
repo_name, 404 unknown file) and response JSON shape must not change by a
single key or status code.

Follows the house pattern in tests/test_export_to_cloud_endpoint.py: import
``main`` (conftest puts orchestrator/ on sys.path), patch its module
globals with an ExitStack, and call the endpoint coroutine directly (no HTTP
transport needed — ``require_job_access`` is replaced on the route
dependencies to skip auth/DB entirely).
"""

from contextlib import ExitStack
import dataclasses
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import job_diff as job_diff_routes
from orchestrator.application import workspace as workspace_composition

# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #

JOB_ID = "682baab8-7864-4f3b-9298-4134294f703e"
REPO_NAME = "job-682baab8"
BASELINE = "aaa1111111111111111111111111111111111111"
HEAD = "bbb2222222222222222222222222222222222222"
BRANCH = "job/682baab8"

# Baseline tree: keep.py (unchanged), old.py (removed at head -> "deleted"),
# mod.py (blob sha changes at head -> "modified"). outside/file.py is NOT
# under projects/ and must never surface in the diff regardless of status.
BASE_TREE = [
    {"path": "projects/proj1/keep.py", "type": "blob", "sha": "s-keep"},
    {"path": "projects/proj1/old.py", "type": "blob", "sha": "s-old"},
    {"path": "projects/proj1/mod.py", "type": "blob", "sha": "s-mod-old"},
    {"path": "outside/file.py", "type": "blob", "sha": "s-outside"},
]

# Head tree: keep.py unchanged, mod.py sha changed ("modified"), new.py only
# here ("added"), old.py absent ("deleted" — inferred from its absence).
HEAD_TREE = [
    {"path": "projects/proj1/keep.py", "type": "blob", "sha": "s-keep"},
    {"path": "projects/proj1/mod.py", "type": "blob", "sha": "s-mod-new"},
    {"path": "projects/proj1/new.py", "type": "blob", "sha": "s-new"},
    {"path": "outside/file.py", "type": "blob", "sha": "s-outside2"},
]


def _make_user() -> dict:
    return {
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "keycloak_sub": "kc-sub-1",
        "email": "user@example.test",
        "display_name": "User One",
        "preferred_username": "user1",
    }


def _make_job(**over) -> dict:
    job = {
        "id": JOB_ID,
        "status": "pending_review",
        "diff_status": "pending",
        "cloud_diff_baseline_commit": BASELINE,
        "repo_name": REPO_NAME,
        "branch_name": BRANCH,
    }
    job.update(over)
    return job


def _make_gitea(
    *,
    initialized: bool = True,
    head_sha: str = HEAD,
    base_tree=None,
    head_tree=None,
    file_contents: dict | None = None,
):
    """MagicMock Gitea client wired for the diff endpoints.

    ``file_contents`` maps ``(path, ref) -> content str``; a missing key
    resolves to ``None``, mirroring ``get_file_content``'s not-found return.
    """
    gitea = MagicMock()
    gitea.is_initialized = initialized
    gitea.get_branch_head_sha = AsyncMock(return_value=head_sha)

    trees = {
        BASELINE: base_tree if base_tree is not None else BASE_TREE,
        head_sha: head_tree if head_tree is not None else HEAD_TREE,
    }

    async def _list_tree(repo_name, ref):
        return trees.get(ref)

    gitea.list_tree = AsyncMock(side_effect=_list_tree)

    contents = file_contents or {}

    async def _get_file_content(repo_name, path, ref=None):
        return contents.get((path, ref))

    gitea.get_file_content = AsyncMock(side_effect=_get_file_content)
    return gitea


def _patch_endpoint(*, user: dict, job: dict, gitea) -> ExitStack:
    # ``user``/``job`` are the pair the caller hands to ``_deps`` — the access
    # gate lives on the route dependencies, not on a patchable global.
    stack = ExitStack()
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.gitea_client", gitea)
    )
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.postgres_db", MagicMock())
    )
    return stack


def _deps(user: dict, job: dict):
    """Route dependencies built from the patched globals, auth short-circuited.

    ``require_job_access`` is a field default on ``JobDiffDependencies``, so
    the gate is replaced here rather than patched on a module the router
    never reads. Call inside the ``_patch_endpoint`` stack — the factory
    reads ``main``'s globals when it runs.
    """
    return dataclasses.replace(
        workspace_composition.job_diff_dependencies(
            orchestrator.main.app.state.resources
        ),
        require_job_access=AsyncMock(return_value=(user, job)),
    )


# --------------------------------------------------------------------------- #
# GET /api/jobs/{job_id}/diff
# --------------------------------------------------------------------------- #


class TestDiffSummary:
    @pytest.mark.asyncio
    async def test_diff_summary_shape(self, fake_request):
        user = _make_user()
        job = _make_job()
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack:
            result = await job_diff_routes.get_job_diff(
                fake_request, JOB_ID, dependencies=_deps(user, job)
            )

        assert result == {
            "job_id": JOB_ID,
            "diff_status": "pending",
            "baseline_commit": BASELINE,
            "head_commit": HEAD,
            "files": [
                {"path": "projects/proj1/mod.py", "status": "modified"},
                {"path": "projects/proj1/new.py", "status": "added"},
                {"path": "projects/proj1/old.py", "status": "deleted"},
            ],
        }

    @pytest.mark.asyncio
    async def test_diff_summary_404_without_baseline_commit(self, fake_request):
        user = _make_user()
        job = _make_job(cloud_diff_baseline_commit=None)
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff(
                fake_request, JOB_ID, dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 404
        assert "baseline" in ei.value.detail.lower()

    @pytest.mark.asyncio
    async def test_diff_summary_503_gitea_not_initialized(self, fake_request):
        user = _make_user()
        job = _make_job()
        gitea = _make_gitea(initialized=False)
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff(
                fake_request, JOB_ID, dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 503

    @pytest.mark.asyncio
    async def test_diff_summary_404_when_no_repo_name(self, fake_request):
        # No repo_name -> get_diff_summary(...) returns None -> 404.
        user = _make_user()
        job = _make_job(repo_name=None)
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff(
                fake_request, JOB_ID, dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 404
        assert "unavailable" in ei.value.detail.lower()


# --------------------------------------------------------------------------- #
# GET /api/jobs/{job_id}/diff/{file_path:path}
# --------------------------------------------------------------------------- #


class TestDiffFile:
    @pytest.mark.asyncio
    async def test_diff_file_modified_reads_old_at_baseline_new_at_branch(
        self, fake_request
    ):
        user = _make_user()
        job = _make_job()
        path = "projects/proj1/mod.py"
        gitea = _make_gitea(
            file_contents={
                (path, BASELINE): "old body",
                (path, BRANCH): "new body",
            }
        )
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack:
            result = await job_diff_routes.get_job_diff_file(
                fake_request, JOB_ID, path, dependencies=_deps(user, job)
            )

        assert result == {
            "job_id": JOB_ID,
            "path": path,
            "status": "modified",
            "old_content": "old body",
            "new_content": "new body",
        }
        assert gitea.get_file_content.await_count == 2
        gitea.get_file_content.assert_any_call(REPO_NAME, path, ref=BASELINE)
        gitea.get_file_content.assert_any_call(REPO_NAME, path, ref=BRANCH)

    @pytest.mark.asyncio
    async def test_diff_file_added_has_none_old(self, fake_request):
        user = _make_user()
        job = _make_job()
        path = "projects/proj1/new.py"
        gitea = _make_gitea(file_contents={(path, BRANCH): "brand new"})
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack:
            result = await job_diff_routes.get_job_diff_file(
                fake_request, JOB_ID, path, dependencies=_deps(user, job)
            )

        assert result["status"] == "added"
        assert result["old_content"] is None
        assert result["new_content"] == "brand new"
        # Old side is never fetched for an added file.
        gitea.get_file_content.assert_awaited_once_with(REPO_NAME, path, ref=BRANCH)

    @pytest.mark.asyncio
    async def test_diff_file_deleted_has_none_new(self, fake_request):
        user = _make_user()
        job = _make_job()
        path = "projects/proj1/old.py"
        gitea = _make_gitea(file_contents={(path, BASELINE): "gone soon"})
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack:
            result = await job_diff_routes.get_job_diff_file(
                fake_request, JOB_ID, path, dependencies=_deps(user, job)
            )

        assert result["status"] == "deleted"
        assert result["old_content"] == "gone soon"
        assert result["new_content"] is None
        # New side is never fetched for a deleted file.
        gitea.get_file_content.assert_awaited_once_with(REPO_NAME, path, ref=BASELINE)

    @pytest.mark.asyncio
    async def test_diff_file_400_outside_projects_prefix(self, fake_request):
        user = _make_user()
        job = _make_job()
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request, JOB_ID, "outside/file.py", dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 400

    @pytest.mark.asyncio
    async def test_diff_file_404_without_baseline_commit(self, fake_request):
        user = _make_user()
        job = _make_job(cloud_diff_baseline_commit=None)
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request,
                JOB_ID,
                "projects/proj1/mod.py",
                dependencies=_deps(user, job),
            )
        assert ei.value.status_code == 404
        assert "baseline" in ei.value.detail.lower()

    @pytest.mark.asyncio
    async def test_diff_file_404_baseline_gate_precedes_400_prefix_gate(
        self, fake_request
    ):
        # Gate order: the no-baseline check (404) fires before the
        # projects/-prefix check (400), even for a path that would also
        # fail the prefix gate.
        user = _make_user()
        job = _make_job(cloud_diff_baseline_commit=None)
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request, JOB_ID, "outside/file.py", dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 404

    @pytest.mark.asyncio
    async def test_diff_file_503_gitea_not_initialized(self, fake_request):
        user = _make_user()
        job = _make_job()
        gitea = _make_gitea(initialized=False)
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request,
                JOB_ID,
                "projects/proj1/mod.py",
                dependencies=_deps(user, job),
            )
        assert ei.value.status_code == 503

    @pytest.mark.asyncio
    async def test_diff_file_404_no_repo_name(self, fake_request):
        user = _make_user()
        job = _make_job(repo_name=None)
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request,
                JOB_ID,
                "projects/proj1/mod.py",
                dependencies=_deps(user, job),
            )
        assert ei.value.status_code == 404
        assert "repo" in ei.value.detail.lower()

    @pytest.mark.asyncio
    async def test_diff_file_404_unknown_file(self, fake_request):
        user = _make_user()
        job = _make_job()
        gitea = _make_gitea()
        stack = _patch_endpoint(user=user, job=job, gitea=gitea)
        with stack, pytest.raises(HTTPException) as ei:
            await job_diff_routes.get_job_diff_file(
                fake_request,
                JOB_ID,
                "projects/proj1/does-not-exist.py",
                dependencies=_deps(user, job),
            )
        assert ei.value.status_code == 404
        assert "not in the diff" in ei.value.detail


# --------------------------------------------------------------------------- #
# GiteaDiffSource memoization (services/diff_source.py)
# --------------------------------------------------------------------------- #


class TestGiteaDiffSourceMemoization:
    """Pin the per-instance summary memo — one Gitea tree walk, not two.

    The per-file endpoint calls ``summary()`` for gating and ``file()``
    (which calls ``summary()`` again) for content on the SAME instance;
    without the memo that's 6 Gitea round trips per file view instead
    of 3 (head-sha lookup + two tree listings, each doubled).
    """

    @pytest.mark.asyncio
    async def test_second_summary_call_does_not_refetch(self):
        from orchestrator.services.diff_source import GiteaDiffSource

        gitea = _make_gitea()
        source = GiteaDiffSource(job=_make_job(), gitea_client=gitea)

        first = await source.summary()
        second = await source.summary()

        assert first is second  # same cached object, not a re-build
        assert gitea.get_branch_head_sha.await_count == 1
        assert gitea.list_tree.await_count == 2  # one walk = base + head trees

    @pytest.mark.asyncio
    async def test_none_summary_is_cached_too(self):
        from orchestrator.services.diff_source import GiteaDiffSource

        gitea = _make_gitea()
        source = GiteaDiffSource(job=_make_job(), gitea_client=gitea)

        with patch(
            "orchestrator.services.job_cloud_baseline.get_diff_summary",
            AsyncMock(return_value=None),
        ) as gds:
            assert await source.summary() is None
            assert await source.summary() is None
            assert gds.await_count == 1  # None result memoized via sentinel

    @pytest.mark.asyncio
    async def test_file_reuses_summary_from_same_instance(self):
        from orchestrator.services.diff_source import GiteaDiffSource

        path = "projects/proj1/mod.py"
        gitea = _make_gitea(
            file_contents={(path, BASELINE): "old", (path, BRANCH): "new"}
        )
        source = GiteaDiffSource(job=_make_job(), gitea_client=gitea)

        await source.summary()  # endpoint's gating call
        content = await source.file(path)  # endpoint's content call

        assert content is not None
        assert content.old_content == "old"
        assert content.new_content == "new"
        # Still exactly one tree walk across both calls.
        assert gitea.get_branch_head_sha.await_count == 1
        assert gitea.list_tree.await_count == 2
