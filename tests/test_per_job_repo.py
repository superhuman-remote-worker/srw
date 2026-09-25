"""Tests for the per-job repo model (resolve_job_repo, _graft_subjob_output, etc.)."""

from tests import _b09_control_seams as control_seams

from tests import b08_completion_helpers as b08_helpers

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from orchestrator.services import subjob_output as subjob_output_module

# main.py requires VECTOR_DB_URL at module level
os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main as main  # noqa: E402

ACCESS = "orchestrator.security.access"
TRIGGER_DISPATCH = "orchestrator.services.job_dispatcher.trigger_dispatch"
SNAPSHOT_SERVICE = "orchestrator.services.snapshot_service.snapshot_service"


def _patch_resource(name: str, *args, **kwargs):
    """Patch one of the application's resources (the former main globals)."""
    return patch.object(main.app.state.resources, name, *args, **kwargs)


DELETE_RESULT = {
    "status": "deleted",
    "ticket_claim_retained": False,
    "ticket_rearmed": False,
    "message": "Job deleted. This job had no durable backlog-ticket claim.",
}


def _stub_request() -> MagicMock:
    """A minimal Request stub for endpoints whose Track A/B gates need one.
    Gates are patched separately to bypass auth — this just lets the
    handler signature line up."""
    req = MagicMock()
    req.headers = {}
    req.cookies = {}
    return req


def _bypass_job_access_gate(job: dict):
    """Patch `require_job_access` to return (admin_caller, job) so the
    destructive owner-or-admin check in delete_job is satisfied without
    standing up the full auth stack."""
    admin = {"id": "00000000-0000-0000-0000-000000000099", "is_admin": True}
    return patch(
        f"{ACCESS}.require_job_access",
        AsyncMock(return_value=(admin, job)),
    )


def _bypass_require_internal():
    """Patch `require_internal` to pass through. P4b agent-internal
    endpoints (subjob_merge etc.) gate on this."""
    return patch(f"{ACCESS}.require_internal", AsyncMock(return_value=None))


# ===========================================================================
# resolve_job_repo
# ===========================================================================


class TestResolveJobRepo:
    """Tests for resolve_job_repo()."""

    @pytest.mark.asyncio
    async def test_job_not_found_raises_404(self):
        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await b08_helpers.resolve_job_repo("nonexistent-id")
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_root_job_with_repo_name(self):
        """Root job has repo_name stored — should return it directly."""
        job = {"repo_name": "job-abcd1234", "branch_name": None}
        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(return_value=job)

            repo, branch = await b08_helpers.resolve_job_repo("abcd1234-xxxx")
            assert repo == "job-abcd1234"
            assert branch is None

    @pytest.mark.asyncio
    async def test_subjob_with_repo_name(self):
        """Subjob has repo_name + branch_name stored on itself."""
        job = {
            "repo_name": "job-parent12",
            "branch_name": "subjob/abcd1234/creator",
            "parent_job_id": "parent-uuid",
        }
        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(return_value=job)

            repo, branch = await b08_helpers.resolve_job_repo("abcd1234-xxxx")
            assert repo == "job-parent12"
            assert branch == "subjob/abcd1234/creator"

    @pytest.mark.asyncio
    async def test_subjob_traverses_to_parent(self):
        """Subjob without repo_name traverses parent_job_id to find repo."""
        subjob = {
            "repo_name": None,
            "branch_name": "subjob/sub12345/validator",
            "parent_job_id": "parent-uuid",
        }
        parent = {"repo_name": "job-parent12", "branch_name": None}

        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(
                side_effect=lambda jid: {
                    "sub12345-xxxx": subjob,
                    "parent-uuid": parent,
                }.get(jid)
            )

            repo, branch = await b08_helpers.resolve_job_repo("sub12345-xxxx")
            assert repo == "job-parent12"
            assert branch == "subjob/sub12345/validator"

    @pytest.mark.asyncio
    async def test_legacy_project_jobs_repo_fallback(self):
        """Pre-migration job with project_id falls back to project jobs repo."""
        job = {
            "repo_name": None,
            "branch_name": "job/legacy-branch",
            "parent_job_id": None,
            "project_id": "proj-uuid",
        }

        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.get_project_repositories = AsyncMock(
                return_value=[{"name": "my-project-jobs"}]
            )

            repo, branch = await b08_helpers.resolve_job_repo("legacy-id")
            assert repo == "my-project-jobs"
            assert branch == "job/legacy-branch"
            mock_db.get_project_repositories.assert_awaited_once_with(
                "proj-uuid", role="jobs"
            )

    @pytest.mark.asyncio
    async def test_non_project_legacy_fallback(self):
        """Job with no repo_name, no parent, no project falls back to job-{id}."""
        job = {
            "repo_name": None,
            "branch_name": None,
            "parent_job_id": None,
            "project_id": None,
        }

        with _patch_resource("postgres_db") as mock_db:
            mock_db.get_job = AsyncMock(return_value=job)

            repo, branch = await b08_helpers.resolve_job_repo("full-uuid-here")
            assert repo == "job-full-uuid-here"
            assert branch is None


# ===========================================================================
# delete_job Gitea cleanup
# ===========================================================================


class TestDeleteJobGiteaCleanup:
    """Tests for delete_job() Gitea branch/repo cleanup logic."""

    @pytest.fixture(autouse=True)
    def vector_retirement(self):
        """Repository tests still complete the required vector retirement."""
        with _patch_resource("vector_db") as vector_db:
            connection = AsyncMock()
            vector_db.acquire.return_value.__aenter__.return_value = connection
            yield connection

    @pytest.mark.asyncio
    async def test_root_job_deletes_repo(self):
        """Root job with repo_name: deletes the entire Gitea repo."""
        job = {
            "repo_name": "job-abcd1234",
            "branch_name": None,
            "parent_job_id": None,
            "project_id": None,
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_job_access_gate(job),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.delete_job = AsyncMock(return_value=True)
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_db.claim_managed_repository_authority_revoke = AsyncMock(
                return_value=None
            )
            mock_db.claim_managed_repository_creation_cleanup = AsyncMock(
                return_value=None
            )
            mock_gitea.is_initialized = True
            mock_gitea.repository_owner = "srw"
            mock_gitea.delete_repo = AsyncMock()
            mock_gitea.delete_branch = AsyncMock()

            result = await control_seams.delete_job(
                _stub_request(), "abcd1234-1111-4111-8111-111111111111"
            )

            assert result == DELETE_RESULT
            mock_gitea.delete_repo.assert_awaited_once_with(
                "job-abcd1234", intent_marker=None
            )
            mock_gitea.delete_branch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_subjob_deletes_branch(self):
        """Subjob with branch_name + repo_name: deletes branch only."""
        job = {
            "repo_name": "job-parent12",
            "branch_name": "subjob/abcd1234/creator",
            "parent_job_id": "parent-uuid",
            "project_id": None,
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_job_access_gate(job),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.delete_job = AsyncMock(return_value=True)
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_gitea.is_initialized = True
            mock_gitea.delete_branch = AsyncMock()
            mock_gitea.delete_repo = AsyncMock()

            result = await control_seams.delete_job(
                _stub_request(), "abcd1234-1111-4111-8111-111111111111"
            )

            assert result == DELETE_RESULT
            mock_gitea.delete_branch.assert_awaited_once_with(
                "job-parent12", "subjob/abcd1234/creator"
            )
            mock_gitea.delete_repo.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_legacy_project_job_deletes_branch(self):
        """Legacy job with project_id + branch_name but no repo_name."""
        job = {
            "repo_name": None,
            "branch_name": "job/legacy-branch",
            "parent_job_id": None,
            "project_id": "proj-uuid",
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_job_access_gate(job),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.delete_job = AsyncMock(return_value=True)
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_db.get_project_repositories = AsyncMock(
                return_value=[{"name": "project-jobs-repo"}]
            )
            mock_gitea.is_initialized = True
            mock_gitea.delete_branch = AsyncMock()
            mock_gitea.delete_repo = AsyncMock()

            result = await control_seams.delete_job(
                _stub_request(), "12345678-1111-4111-8111-111111111111"
            )

            assert result == DELETE_RESULT
            mock_gitea.delete_branch.assert_awaited_once_with(
                "project-jobs-repo", "job/legacy-branch"
            )

    @pytest.mark.asyncio
    async def test_legacy_stamped_shared_repo_is_never_deleted_with_job(self):
        """Later legacy rows stored the shared repo name directly on the job.

        The presence of ``repo_name`` must not make that shared project repo
        look job-owned during migration cleanup.
        """
        job = {
            "id": "12345678-1111-2222-3333-444444444444",
            "repo_name": "project-68137e29-jobs",
            "branch_name": "job/12345678",
            "parent_job_id": None,
            "project_id": "68137e29-1111-2222-3333-444444444444",
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_job_access_gate(job),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.delete_job = AsyncMock(return_value=True)
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_gitea.is_initialized = True
            mock_gitea.delete_branch = AsyncMock()
            mock_gitea.delete_repo = AsyncMock()

            result = await control_seams.delete_job(_stub_request(), str(job["id"]))

        assert result == DELETE_RESULT
        mock_gitea.delete_branch.assert_awaited_once_with(
            "project-68137e29-jobs", "job/12345678"
        )
        mock_gitea.delete_repo.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_gitea_not_initialized_retains_owner_for_revocation_retry(self):
        """An unavailable forge may not orphan a still-usable repository key."""
        job = {"repo_name": "job-abc", "branch_name": None, "parent_job_id": None}

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_job_access_gate(job),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_db.delete_job = AsyncMock(return_value=True)
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_db.claim_managed_repository_authority_revoke = AsyncMock(
                return_value=None
            )
            mock_db.claim_managed_repository_creation_cleanup = AsyncMock(
                return_value=None
            )
            mock_gitea.is_initialized = False
            mock_gitea.repository_owner = "srw"
            mock_gitea.delete_repo = AsyncMock(return_value=False)

            with pytest.raises(HTTPException) as exc:
                await control_seams.delete_job(_stub_request(), "some-id")

            assert exc.value.status_code == 503
            mock_db.delete_job.assert_not_awaited()
            mock_gitea.delete_repo.assert_awaited_once_with(
                "job-abc", intent_marker=None
            )
            mock_gitea.delete_branch.assert_not_called()

    @pytest.mark.asyncio
    async def test_stateless_delete_fences_and_prunes_before_resource_cleanup(self):
        job = {
            "id": "12345678-1111-2222-3333-444444444444",
            "execution_lane": "stateless",
            "repo_name": None,
            "branch_name": None,
            "parent_job_id": None,
            "project_id": None,
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            patch(SNAPSHOT_SERVICE) as mock_snapshots,
            patch(
                "orchestrator.services.thread_retirement.ThreadRetirementOperations.archive_and_cleanup_workspace"
            ) as cleanup_workspace,
            _bypass_job_access_gate(job),
        ):
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.prepare_stateless_job_for_delete = AsyncMock(return_value=True)

            async def cleanup_after_fence(_job_id):
                mock_db.prepare_stateless_job_for_delete.assert_awaited_once_with(
                    str(job["id"])
                )

            cleanup_workspace.side_effect = cleanup_after_fence

            async def delete_after_fence(
                _job_id,
                *,
                prepared_stateless=False,
                deletion_actor_user_id=None,
                deletion_reason=None,
                return_claim_state=False,
            ):
                mock_db.prepare_stateless_job_for_delete.assert_awaited_once()
                assert prepared_stateless is True
                assert deletion_actor_user_id == (
                    "00000000-0000-0000-0000-000000000099"
                )
                assert deletion_reason == "authorized_api_delete"
                assert return_claim_state is True
                return {"deleted": True, "ticket_claim_retained": False}

            mock_db.delete_job = AsyncMock(side_effect=delete_after_fence)
            mock_db.job_has_durable_ticket_claim = AsyncMock(return_value=False)
            mock_gitea.is_initialized = False
            mock_snapshots.is_available = False

            result = await control_seams.delete_job(_stub_request(), str(job["id"]))

        assert result == DELETE_RESULT
        cleanup_workspace.assert_awaited_once_with(str(job["id"]))
        mock_db.delete_job.assert_awaited_once_with(
            str(job["id"]),
            prepared_stateless=True,
            deletion_actor_user_id="00000000-0000-0000-0000-000000000099",
            deletion_reason="authorized_api_delete",
            return_claim_state=True,
        )

    @pytest.mark.asyncio
    async def test_stateless_delete_prepare_failure_keeps_resources_intact(self):
        job = {
            "id": "12345678-1111-2222-3333-444444444444",
            "execution_lane": "stateless",
            "parent_job_id": None,
            "project_id": None,
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            patch(
                "orchestrator.services.thread_retirement.ThreadRetirementOperations.archive_and_cleanup_workspace"
            ) as cleanup_workspace,
            _bypass_job_access_gate(job),
        ):
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.prepare_stateless_job_for_delete = AsyncMock(return_value=False)
            mock_db.delete_job = AsyncMock()

            with pytest.raises(HTTPException) as exc_info:
                await control_seams.delete_job(_stub_request(), str(job["id"]))

        assert exc_info.value.status_code == 409
        cleanup_workspace.assert_not_awaited()
        mock_db.delete_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stateless_delete_strict_prune_error_keeps_resources_intact(self):
        job = {
            "id": "12345678-1111-2222-3333-444444444444",
            "execution_lane": "stateless",
            "parent_job_id": None,
            "project_id": None,
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            patch(
                "orchestrator.services.thread_retirement.ThreadRetirementOperations.archive_and_cleanup_workspace"
            ) as cleanup_workspace,
            _bypass_job_access_gate(job),
        ):
            mock_db.has_child_jobs = AsyncMock(return_value=False)
            mock_db.prepare_stateless_job_for_delete = AsyncMock(
                side_effect=RuntimeError("strict prune failed")
            )
            mock_db.delete_job = AsyncMock()

            with pytest.raises(HTTPException) as exc_info:
                await control_seams.delete_job(_stub_request(), str(job["id"]))

        assert exc_info.value.status_code == 500
        assert "strict prune failed" in str(exc_info.value.detail)
        cleanup_workspace.assert_not_awaited()
        mock_db.delete_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_not_found_raises_404(self):
        # The gate (`require_job_access`) is what raises 404 for missing
        # jobs now — let the real helper run with a patched db that returns None.
        with (
            _patch_resource("postgres_db") as mock_db,
            patch(
                "orchestrator.security.access.require_approved_user",
                AsyncMock(
                    return_value={
                        "id": "00000000-0000-0000-0000-000000000099",
                        "is_admin": True,
                    }
                ),
            ),
        ):
            mock_db.get_job = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await control_seams.delete_job(_stub_request(), "missing-id")
            assert exc_info.value.status_code == 404


# ===========================================================================
# subjob_merge endpoint
# ===========================================================================


class TestSubjobMergeEndpoint:
    """Tests for POST /api/jobs/{job_id}/subjob-merge."""

    @pytest.mark.asyncio
    async def test_rejects_non_subjob(self):
        """Root jobs (no parent_job_id) should be rejected with 400."""
        job = {"parent_job_id": None}

        with (
            _patch_resource("postgres_db") as mock_db,
            _bypass_require_internal(),
        ):
            mock_db.get_job = AsyncMock(return_value=job)

            with pytest.raises(HTTPException) as exc_info:
                await control_seams.subjob_merge(_stub_request(), "root-job-id")
            assert exc_info.value.status_code == 400

    @pytest.mark.asyncio
    async def test_returns_404_for_missing_job(self):
        with (
            _patch_resource("postgres_db") as mock_db,
            _bypass_require_internal(),
        ):
            mock_db.get_job = AsyncMock(return_value=None)

            with pytest.raises(HTTPException) as exc_info:
                await control_seams.subjob_merge(_stub_request(), "missing-id")
            assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_skipped_when_no_branch(self):
        """Stateless subjob merge is unchanged by the /complete lease fence."""
        job = {
            "parent_job_id": "parent-id",
            "branch_name": None,
            "repo_name": None,
            "execution_lane": "stateless",
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            _patch_resource("gitea_client") as mock_gitea,
            _bypass_require_internal(),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_gitea.is_initialized = True

            result = await control_seams.subjob_merge(_stub_request(), "subjob-id")
            assert result["status"] == "skipped"

    @pytest.mark.asyncio
    async def test_returns_graft_result(self):
        """Happy path: returns graft result from _graft_subjob_output."""
        job = {
            "parent_job_id": "parent-id",
            "branch_name": "subjob/abc/scholar",
            "repo_name": "job-parent",
        }

        with (
            _patch_resource("postgres_db") as mock_db,
            patch.object(
                subjob_output_module,
                "graft_subjob_output",
                new_callable=AsyncMock,
            ) as mock_graft,
            _bypass_require_internal(),
        ):
            mock_db.get_job = AsyncMock(return_value=job)
            mock_graft.return_value = {
                "status": "grafted",
                "output_path": "outputs/001-scholar-abc",
            }

            result = await control_seams.subjob_merge(_stub_request(), "subjob-id")
            assert result["status"] == "grafted"
            assert result["job_id"] == "subjob-id"
            assert result["output_path"] == "outputs/001-scholar-abc"


# ===========================================================================
# _next_output_ordinal
# ===========================================================================


class _OutputsFake:
    """Minimal gitea fake exposing list_contents for an `outputs/` dir."""

    def __init__(self, outputs_dirs: list[str]):
        # outputs_dirs: directory names directly under outputs/, e.g. ["001-scholar-aa"]
        self._dirs = outputs_dirs
        self.is_initialized = True

    async def list_contents(self, repo, path="", ref=None):
        if path != "outputs":
            return []
        return [{"name": d, "path": f"outputs/{d}", "type": "dir"} for d in self._dirs]


class TestNextOutputOrdinal:
    @pytest.mark.asyncio
    async def test_first_ordinal_is_001(self):
        fake = _OutputsFake([])
        with _patch_resource("gitea_client", fake):
            assert await b08_helpers.next_output_ordinal("job-x", "main") == "001"

    @pytest.mark.asyncio
    async def test_increments_past_highest(self):
        fake = _OutputsFake(["001-scholar-aa", "002-critic-bb", "010-developer-cc"])
        with _patch_resource("gitea_client", fake):
            assert await b08_helpers.next_output_ordinal("job-x", "main") == "011"

    @pytest.mark.asyncio
    async def test_ignores_non_numbered_entries(self):
        fake = _OutputsFake(["notes", "003-scholar-dd"])
        with _patch_resource("gitea_client", fake):
            assert await b08_helpers.next_output_ordinal("job-x", "main") == "004"


import base64 as _b64  # noqa: E402

# ===========================================================================
# _graft_subjob_output
# ===========================================================================


class _GraftFakeGitea:
    """Models per-branch trees as {branch: {path: bytes}} and the graft I/O.

    - list_tree(ref) -> [{path, type:"blob"}] for that branch
    - get_file_bytes(path, ref) -> bytes
    - list_contents("outputs", ref) -> dir entries under outputs/ on that branch
    - change_files(branch, files) -> add files (base64) to that branch's tree
    """

    def __init__(self, trees: dict[str, dict[str, bytes]]):
        self.trees = {b: dict(t) for b, t in trees.items()}
        self.is_initialized = True
        self.commits: dict[str, list[dict[str, str]]] = {}
        self.commit_probe_available = True
        self.change_calls: list[dict[str, object]] = []

    async def list_tree(self, repo, ref):
        return [{"path": p, "type": "blob"} for p in self.trees.get(ref, {})]

    async def get_file_bytes(self, repo, file_path, ref=None):
        return self.trees.get(ref, {}).get(file_path)

    async def list_contents(self, repo, path="", ref=None):
        if path != "outputs":
            return []
        names = set()
        for p in self.trees.get(ref, {}):
            if p.startswith("outputs/"):
                names.add(p.split("/")[1])
        return [{"name": n, "path": f"outputs/{n}", "type": "dir"} for n in names]

    async def change_files(self, repo, branch, files, message):
        self.change_calls.append(
            {"repo": repo, "branch": branch, "files": files, "message": message}
        )
        tree = self.trees.setdefault(branch, {})
        for f in files:
            tree[f["path"]] = _b64.b64decode(f["content_b64"])
        self.commits.setdefault(branch, []).insert(
            0,
            {"sha": f"commit-{len(self.change_calls)}", "message": message},
        )
        return True

    async def get_commits(self, repo, sha="main", page=1, limit=20):
        if not self.commit_probe_available:
            return None
        start = (page - 1) * limit
        return self.commits.get(sha, [])[start : start + limit]


def _subjob(**over):
    base = {
        "id": "sub-uuid-1234abcd",
        "parent_job_id": "parent-uuid",
        "branch_name": "subjob/1234abcd/scholar",
        "repo_name": "job-parent12",
        "config_name": "scholar",
        "description": "research",
        "context": {},
    }
    base.update(over)
    return base


class TestGraftSubjobOutput:
    COMMAND_ID = "12345678-1234-5678-9abc-123456789abc"

    @pytest.mark.asyncio
    async def test_grafts_output_to_namespaced_dir_and_leaves_parent_untouched(self):
        fake = _GraftFakeGitea(
            {
                "main": {"documents/corpus.pdf": b"PARENT", "src/app.py": b"code"},
                "subjob/1234abcd/scholar": {
                    "documents/corpus.pdf": b"PARENT",  # inherited from fork
                    "src/app.py": b"code",
                    "output/ideas/idea.md": b"# idea",
                    "output/report.pdf": b"\x89PDFbytes",
                    "workspace.md": b"scratch",  # NOT under output/
                },
            }
        )
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda j: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(j)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output("sub-uuid-1234abcd")

        assert result["status"] == "grafted"
        assert result["output_path"] == "outputs/001-scholar-sub-uuid"
        # output/ contents relocated under the namespaced dir, prefix stripped:
        assert (
            fake.trees["main"]["outputs/001-scholar-sub-uuid/ideas/idea.md"]
            == b"# idea"
        )
        assert (
            fake.trees["main"]["outputs/001-scholar-sub-uuid/report.pdf"]
            == b"\x89PDFbytes"
        )
        # parent content untouched; scratch + inherited tree NOT propagated:
        assert fake.trees["main"]["documents/corpus.pdf"] == b"PARENT"
        assert "outputs/001-scholar-sub-uuid/workspace.md" not in fake.trees["main"]
        db.update_job_merge_status.assert_awaited_with(
            "sub-uuid-1234abcd", merge_status="grafted"
        )

    @pytest.mark.asyncio
    async def test_critic_grafts_nothing(self):
        fake = _GraftFakeGitea(
            {
                "main": {"src/app.py": b"code"},
                "subjob/1234abcd/critic": {"output/reviews/r.md": b"review"},
            }
        )
        critic = _subjob(
            config_name="critic",
            branch_name="subjob/1234abcd/critic",
            context={"verification_target": "parent-uuid"},
        )
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda j: {
                    "sub-uuid-1234abcd": critic,
                    "parent-uuid": {"branch_name": None},
                }.get(j)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output("sub-uuid-1234abcd")

        assert result == {"status": "skipped", "reason": "critic-not-merged"}
        assert all(not k.startswith("outputs/") for k in fake.trees["main"])

    @pytest.mark.asyncio
    async def test_no_output_skipped(self):
        fake = _GraftFakeGitea(
            {"main": {}, "subjob/1234abcd/scholar": {"workspace.md": b"scratch"}}
        )
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda j: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(j)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output("sub-uuid-1234abcd")

        assert result == {"status": "skipped", "reason": "no-output"}

    @pytest.mark.asyncio
    async def test_ordinal_increments_when_outputs_exist(self):
        fake = _GraftFakeGitea(
            {
                "main": {"outputs/001-scholar-old/x.md": b"old"},
                "subjob/1234abcd/scholar": {"output/y.md": b"new"},
            }
        )
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda j: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(j)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output("sub-uuid-1234abcd")

        assert result["output_path"] == "outputs/002-scholar-sub-uuid"
        assert fake.trees["main"]["outputs/002-scholar-sub-uuid/y.md"] == b"new"

    @pytest.mark.asyncio
    async def test_already_grafted_is_skipped(self):
        # Re-invoking after a graft must NOT copy the output again under a new
        # ordinal. The recorded graft_output_path short-circuits the function.
        fake = _GraftFakeGitea(
            {
                "main": {"outputs/001-scholar-sub-uuid/x.md": b"prev"},
                "subjob/1234abcd/scholar": {"output/y.md": b"new"},
            }
        )
        already = _subjob(context={"graft_output_path": "outputs/001-scholar-sub-uuid"})
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda j: {
                    "sub-uuid-1234abcd": already,
                    "parent-uuid": {"branch_name": None},
                }.get(j)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output("sub-uuid-1234abcd")

        assert result["status"] == "skipped"
        assert result["reason"] == "already-grafted"
        assert result["output_path"] == "outputs/001-scholar-sub-uuid"
        # No second ordinal folder created; context not rewritten.
        assert not any(k.startswith("outputs/002-") for k in fake.trees["main"])
        db.merge_job_context.assert_not_called()

    @pytest.mark.asyncio
    async def test_command_key_is_written_as_exact_commit_trailer(self):
        fake = _GraftFakeGitea(
            {
                "main": {},
                "subjob/1234abcd/scholar": {"output/report.md": b"done"},
            }
        )
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda job_id: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(job_id)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output(
                "sub-uuid-1234abcd",
                completion_command_id=self.COMMAND_ID,
            )

        message = str(fake.change_calls[0]["message"])
        assert f"SRW-Completion-Command: {self.COMMAND_ID}" in message
        assert f"SRW-Graft-Output: {result['output_path']}" in message

    @pytest.mark.asyncio
    async def test_command_trailer_reconciles_commit_before_db_marker(self):
        output_path = "outputs/007-scholar-sub-uuid"
        fake = _GraftFakeGitea(
            {
                "main": {f"{output_path}/report.md": b"already committed"},
                "subjob/1234abcd/scholar": {"output/report.md": b"done"},
            }
        )
        fake.commits["main"] = [
            {
                "sha": "deadbeef",
                "message": (
                    f"Graft {output_path} from subjob sub-uuid\n\n"
                    f"SRW-Completion-Command: {self.COMMAND_ID}\n"
                    f"SRW-Graft-Output: {output_path}"
                ),
            }
        ]
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda job_id: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(job_id)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            result = await b08_helpers.graft_subjob_output(
                "sub-uuid-1234abcd",
                completion_command_id=self.COMMAND_ID,
            )

        assert result == {
            "status": "grafted",
            "reason": "reconciled-command-trailer",
            "base_branch": "main",
            "output_path": output_path,
            "commit_sha": "deadbeef",
        }
        assert fake.change_calls == []
        db.update_job_merge_status.assert_awaited_once_with(
            "sub-uuid-1234abcd", merge_status="grafted"
        )
        db.merge_job_context.assert_awaited_once_with(
            "sub-uuid-1234abcd", {"graft_output_path": output_path}
        )

    @pytest.mark.asyncio
    async def test_ambiguous_command_probe_refuses_to_repeat_graft(self):
        from orchestrator.services.completion_effect_reconciliation import (
            CompletionEffectProbeError,
        )

        fake = _GraftFakeGitea(
            {
                "main": {},
                "subjob/1234abcd/scholar": {"output/report.md": b"done"},
            }
        )
        fake.commit_probe_available = False
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda job_id: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(job_id)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            with pytest.raises(CompletionEffectProbeError):
                await b08_helpers.graft_subjob_output(
                    "sub-uuid-1234abcd",
                    completion_command_id=self.COMMAND_ID,
                )

        assert fake.change_calls == []
        db.update_job_merge_status.assert_not_awaited()
        db.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ambiguous_write_result_stays_pending_for_command_probe(self):
        fake = _GraftFakeGitea(
            {
                "main": {},
                "subjob/1234abcd/scholar": {"output/report.md": b"done"},
            }
        )
        fake.change_files = AsyncMock(return_value=False)
        with (
            _patch_resource("postgres_db") as db,
            _patch_resource("gitea_client", fake),
        ):
            db.get_job = AsyncMock(
                side_effect=lambda job_id: {
                    "sub-uuid-1234abcd": _subjob(),
                    "parent-uuid": {"branch_name": None},
                }.get(job_id)
            )
            db.update_job_merge_status = AsyncMock()
            db.merge_job_context = AsyncMock()

            with pytest.raises(
                RuntimeError, match="durable graft write outcome is ambiguous"
            ):
                await b08_helpers.graft_subjob_output(
                    "sub-uuid-1234abcd",
                    completion_command_id=self.COMMAND_ID,
                )

        fake.change_files.assert_awaited_once()
        db.update_job_merge_status.assert_not_awaited()
        db.merge_job_context.assert_not_awaited()


class TestCompletionGraftWiring:
    @pytest.mark.asyncio
    async def test_graft_fires_for_delegation_child(self):
        # A delegation child has creation_order set; the old gate skipped it.
        called = {}

        async def fake_graft(job_id, **_kwargs):
            called["job_id"] = job_id
            return {
                "status": "grafted",
                "output_path": "outputs/001-developer-deadbeef",
            }

        child = {
            "id": "deadbeef-child",
            "parent_job_id": "p",
            "creation_order": 0,
            "branch_name": "subjob/deadbeef/developer",
            "repo_name": "job-p",
            "config_name": "developer",
        }
        with patch.object(
            subjob_output_module,
            "graft_subjob_output",
            side_effect=fake_graft,
        ):
            res = await b08_helpers.maybe_graft_completed_subjob(child)
        assert called["job_id"] == "deadbeef-child"
        assert res["status"] == "grafted"

    @pytest.mark.asyncio
    async def test_no_graft_for_root_job(self):
        with patch.object(
            subjob_output_module,
            "graft_subjob_output",
            new_callable=AsyncMock,
        ) as g:
            res = await b08_helpers.maybe_graft_completed_subjob(
                {"id": "r", "parent_job_id": None}
            )
        assert res is None
        g.assert_not_called()


class TestScholarOutputPointer:
    @pytest.mark.asyncio
    async def test_scholar_completion_sets_parent_output_dir_to_graft_path(self):
        # The in-memory scholar passed to the handler predates the graft's
        # context write, so it LACKS graft_output_path. The DB row (re-fetched)
        # has it. This guards the re-fetch: reading the in-memory ctx yields None.
        scholar_in_memory = {
            "id": "sch-1",
            "parent_job_id": "par-1",
            "status": "completed",
            "context": {"scholar_target": "par-1"},
        }
        scholar_fresh = {
            "id": "sch-1",
            "parent_job_id": "par-1",
            "status": "completed",
            "context": {
                "scholar_target": "par-1",
                "graft_output_path": "outputs/003-scholar-sch1",
            },
        }
        parent = {"id": "par-1", "status": "waiting", "context": {}}
        captured = {}

        async def upd_ctx(jid, ctx):
            captured[jid] = ctx

        with _patch_resource("postgres_db") as db:
            db.get_job = AsyncMock(
                side_effect=lambda j: {"sch-1": scholar_fresh, "par-1": parent}.get(j)
            )
            db.merge_job_context = AsyncMock(side_effect=upd_ctx)
            db.update_job_status = AsyncMock()
            with patch(TRIGGER_DISPATCH):
                await b08_helpers.handle_scholar_completion(scholar_in_memory, [])

        assert captured["par-1"]["scholar_output_dir"] == "outputs/003-scholar-sch1"
        assert captured["par-1"]["scholar_completed"] is True

    @pytest.mark.asyncio
    async def test_paused_scholar_does_not_unblock_parent(self):
        # An outage/drain-paused scholar reports /complete with a NON-terminal
        # status (the pause path sets job["status"]="paused" in-memory before
        # step 3b) — the parent must keep waiting for the resumed scholar's
        # real outcome, not be unblocked as research-success. Live-caught on
        # k3d: a cooldown-paused scholar falsely flipped its parent
        # waiting→created. knowledge-base/knowledge/features/llm_outage_subjob_resilience.md
        scholar = {
            "id": "sch-1",
            "parent_job_id": "par-1",
            "status": "paused",
            "context": {"scholar_target": "par-1"},
        }
        parent = {"id": "par-1", "status": "waiting", "context": {}}
        with _patch_resource("postgres_db") as db:
            db.get_job = AsyncMock(return_value=parent)
            db.merge_job_context = AsyncMock()
            db.update_job_status = AsyncMock()
            with patch(TRIGGER_DISPATCH) as trig:
                await b08_helpers.handle_scholar_completion(scholar, [])
        db.merge_job_context.assert_not_awaited()
        db.update_job_status.assert_not_awaited()
        trig.assert_not_called()


class TestDelegationOutputPathPopulation:
    """The resume builder must surface each child's graft_output_path as output_path."""

    @pytest.mark.asyncio
    async def test_child_results_carry_graft_output_path(self):
        # Graft writes graft_output_path into each child's DB context; the
        # delegation resume builder must read it back as output_path. Locks the
        # producer->consumer key contract for _handle_delegation_child_completion.
        job = {"id": "child-1", "parent_job_id": "par-1", "creation_order": 1}
        parent = {"id": "par-1", "status": "waiting", "context": {}}
        children = [
            {
                "id": "c0",
                "creation_order": 0,
                "status": "completed",
                "config_name": "scholar",
                "branch_name": "subjob/c0/scholar",
                "context": {"graft_output_path": "outputs/001-scholar-c0"},
                "freeze_data": {"summary": "did X"},
            },
            {
                "id": "c1",
                "creation_order": 1,
                "status": "completed",
                "config_name": "developer",
                "branch_name": "subjob/c1/developer",
                "context": {},  # produced no output -> no graft path
                "freeze_data": {},
            },
        ]
        captured = {}

        async def upd_ctx(jid, ctx):
            captured[jid] = ctx

        with _patch_resource("postgres_db") as db:
            db.all_delegation_children_terminal = AsyncMock(return_value=True)
            db.get_job = AsyncMock(side_effect=lambda j: {"par-1": parent}.get(j))
            db.get_delegation_children = AsyncMock(return_value=children)
            db.merge_job_context = AsyncMock(side_effect=upd_ctx)
            db.update_job_status = AsyncMock()
            db.claim_delegation_resume = AsyncMock(return_value=True)
            with patch(TRIGGER_DISPATCH):
                await b08_helpers.handle_delegation_child_completion(job, [])

        results = captured["par-1"]["delegation_results"]
        by_order = {r["creation_order"]: r for r in results}
        assert by_order[0]["output_path"] == "outputs/001-scholar-c0"
        assert by_order[0]["config_name"] == "scholar"
        assert by_order[1]["output_path"] is None


class TestDelegationUnblockDispatcherContract:
    """Unblock must re-queue via the CAS claim that clears freeze_data.

    ``update_job_status(status="paused")`` leaves the parent's delegation
    freeze set, and ``get_dispatchable_jobs`` requires ``freeze_data IS
    NULL`` — the re-queued parent would be dispatcher-invisible forever.
    Regression for knowledge-base/knowledge/issues/delegation_freeze_lifecycle_gaps.md (Gap 1).
    """

    def _fixtures(self):
        job = {"id": "child-1", "parent_job_id": "par-1", "creation_order": 0}
        parent = {"id": "par-1", "status": "waiting", "context": {}}
        children = [
            {
                "id": "child-1",
                "creation_order": 0,
                "status": "completed",
                "config_name": "scholar",
                "branch_name": "subjob/child-1/scholar",
                "context": {},
                "freeze_data": {},
            }
        ]
        return job, parent, children

    @pytest.mark.asyncio
    async def test_unblock_requeues_via_cas_claim(self):
        job, parent, children = self._fixtures()
        with _patch_resource("postgres_db") as db:
            db.all_delegation_children_terminal = AsyncMock(return_value=True)
            db.get_job = AsyncMock(return_value=parent)
            db.get_delegation_children = AsyncMock(return_value=children)
            db.merge_job_context = AsyncMock()
            db.update_job_status = AsyncMock()
            db.claim_delegation_resume = AsyncMock(return_value=True)
            with patch(TRIGGER_DISPATCH) as trig:
                await b08_helpers.handle_delegation_child_completion(job, [])
        db.claim_delegation_resume.assert_awaited_once_with("par-1")
        # update_job_status can't clear freeze_data → must not be the writer
        db.update_job_status.assert_not_awaited()
        trig.assert_called_once()

    @pytest.mark.asyncio
    async def test_losing_cas_skips_dispatch_trigger(self):
        # A concurrent sibling (or the timeout sweeper) already re-queued the
        # parent — the loser must not double-trigger or log a second resume.
        job, parent, children = self._fixtures()
        with _patch_resource("postgres_db") as db:
            db.all_delegation_children_terminal = AsyncMock(return_value=True)
            db.get_job = AsyncMock(return_value=parent)
            db.get_delegation_children = AsyncMock(return_value=children)
            db.merge_job_context = AsyncMock()
            db.update_job_status = AsyncMock()
            db.claim_delegation_resume = AsyncMock(return_value=False)
            with patch(TRIGGER_DISPATCH) as trig:
                await b08_helpers.handle_delegation_child_completion(job, [])
        trig.assert_not_called()

    @pytest.mark.asyncio
    async def test_stateless_parent_reenqueues_without_dispatcher(self):
        job, parent, children = self._fixtures()
        parent.update(
            {
                "execution_lane": "stateless",
                "priority": 9,
                "user_id": "33333333-3333-3333-3333-333333333333",
            }
        )
        with _patch_resource("postgres_db") as db:
            db.all_delegation_children_terminal = AsyncMock(return_value=True)
            db.get_job = AsyncMock(return_value=parent)
            db.get_delegation_children = AsyncMock(return_value=children)
            db.queue_stateless_job_for_resume = AsyncMock(return_value=True)
            db.merge_job_context = AsyncMock()
            db.claim_delegation_resume = AsyncMock()
            with patch(TRIGGER_DISPATCH) as trig:
                await b08_helpers.handle_delegation_child_completion(job, [])

        queued = db.queue_stateless_job_for_resume.await_args
        assert queued.args[0] == "par-1"
        assert queued.args[1]["delegation_results"][0]["job_id"] == "child-1"
        assert queued.kwargs == {
            "priority": 9,
            "fair_key": "33333333-3333-3333-3333-333333333333",
            "expected_status": "waiting",
        }
        db.merge_job_context.assert_not_awaited()
        db.claim_delegation_resume.assert_not_awaited()
        trig.assert_not_called()
