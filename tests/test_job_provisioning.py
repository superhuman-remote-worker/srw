"""Tests for ``orchestrator/services/job_provisioning.py``.

Every root job—project-attached or loose—gets an isolated repository; only
subjobs branch inside their root's repository. The suite also covers the hardened
creator grant (username + full_name + sub), the cloud-baseline seed gate,
the ``is_initialized`` no-op, and — critically — asyncpg UUID coercion
(``job_row`` carries native ``uuid.UUID`` objects that the ``postgres_db``
helpers do ``UUID(arg)`` on and would raise if handed a UUID instance).
DB + Gitea are mocked; no Postgres/Gitea container needed.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.job_provisioning import (
    JobProvisioningError,
    provision_job_repo,
)


class _AsyncCtx:
    """Minimal ``async with`` returning a fixed value (asyncpg pool/conn shape)."""

    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_gitea(*, initialized: bool = True) -> MagicMock:
    g = MagicMock()
    g.is_initialized = initialized
    g.repository_owner = "srw"
    g.create_repo = AsyncMock(return_value="http://srw-gitea:3000/srw/job-xxxx.git")
    g.repository_creation_intent_status = AsyncMock(return_value="missing")
    g.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://srw-gitea:3000/srw/{name}.git"
    )
    g.ensure_repo_deploy_key = AsyncMock(return_value=17)
    g.probe_repo_deploy_key = AsyncMock(return_value=True)
    g.delete_repo_deploy_key = AsyncMock(return_value=True)
    g.delete_repo = AsyncMock(return_value=True)
    g.create_branch = AsyncMock(return_value=True)
    g.grant_user_repo_access = AsyncMock(return_value=True)
    # Loop floor path (loop_floor=True): no .gitignore on main yet → seed.
    g.get_file_bytes = AsyncMock(return_value=None)
    g.change_files = AsyncMock(return_value=True)
    return g


def _make_db(
    *,
    user: dict | None = None,
    project: dict | None = None,
    project_repos: list | None = None,
    parent: dict | None = None,
) -> MagicMock:
    conn = MagicMock()
    conn.execute = AsyncMock()
    db = MagicMock()
    db.acquire = MagicMock(return_value=_AsyncCtx(conn))
    db.merge_job_context = AsyncMock()
    db.bind_job_managed_repository = AsyncMock(return_value=True)
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.get_user = AsyncMock(return_value=user)
    db.get_project = AsyncMock(return_value=project)
    db.get_user_project_roles = AsyncMock(return_value={})
    db.get_project_repositories = AsyncMock(return_value=project_repos or [])
    db.get_job = AsyncMock(return_value=parent)
    authority_id = uuid.uuid4()
    creation_intent_id = uuid.uuid4()
    creation_marker = uuid.uuid4()

    async def reserve_creation_intent(**kwargs):
        return {
            "id": creation_intent_id,
            "intent_marker": creation_marker,
            "status": "pending",
            **kwargs,
        }

    db.reserve_managed_repository_creation_intent = AsyncMock(
        side_effect=reserve_creation_intent
    )
    db.mark_managed_repository_created = AsyncMock(
        side_effect=lambda intent_id, **_kwargs: {
            "id": uuid.UUID(intent_id),
            "intent_marker": creation_marker,
            "status": "created",
        }
    )
    db.fail_managed_repository_creation_intent = AsyncMock(return_value=True)
    db.conflict_managed_repository_creation_intent = AsyncMock(return_value=True)

    async def reserve_authority(**kwargs):
        return {
            "id": authority_id,
            "repository_owner": kwargs["repository_owner"],
            "repo_name": kwargs["repo_name"],
            "authority_kind": kwargs["authority_kind"],
            "authority_id": uuid.UUID(kwargs["authority_id"]),
            "project_id": (
                uuid.UUID(kwargs["project_id"]) if kwargs["project_id"] else None
            ),
            "generation": 1,
            "access_mode": kwargs["access_mode"],
            "creation_intent_id": (
                uuid.UUID(kwargs["creation_intent_id"])
                if kwargs.get("creation_intent_id")
                else None
            ),
            "clean_repo_url": kwargs["clean_repo_url"],
            "public_key": kwargs["public_key"],
            "public_key_fingerprint": kwargs["public_key_fingerprint"],
            "private_key": kwargs["private_key"],
            "status": "active",
        }

    db.reserve_managed_repository_authority = AsyncMock(side_effect=reserve_authority)
    db.claim_managed_repository_authority_revoke = AsyncMock(return_value=None)
    db.finish_managed_repository_authority_revoke = AsyncMock(return_value=True)
    db.claim_managed_repository_creation_cleanup = AsyncMock(
        return_value={
            "id": creation_intent_id,
            "intent_marker": creation_marker,
            "status": "deleting",
        }
    )
    db.finish_managed_repository_creation_cleanup = AsyncMock(return_value=True)
    return db


def _creator() -> dict:
    return {
        "email": "max@stud.fra-uas.de",
        "preferred_username": "max",
        "display_name": "Max Mustermann",
        "keycloak_sub": "d0d7dea6-6c7b-4fb2-9d47-b84096253db3",
    }


@pytest.fixture
def patched_seed(monkeypatch):
    """Patch the locally-imported ``fire_baseline_seed`` so the cloud-seed
    branch never schedules a real background task."""
    seed = MagicMock()
    monkeypatch.setattr(
        "orchestrator.services.job_cloud_baseline.fire_baseline_seed", seed
    )
    return seed


@pytest.fixture
def patched_loop_seed(monkeypatch):
    seed = AsyncMock()
    monkeypatch.setattr(
        "orchestrator.services.job_cloud_baseline.seed_project_folder_baseline", seed
    )
    return seed


class TestProvisionJobRepo:
    @pytest.mark.asyncio
    async def test_noop_when_gitea_uninitialized(self) -> None:
        g = _make_gitea(initialized=False)
        db = _make_db()
        row = {"id": uuid.uuid4(), "user_id": uuid.uuid4()}

        out = await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert out is row
        g.create_repo.assert_not_called()
        g.create_branch.assert_not_called()
        db.get_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_standalone_root_creates_repo_and_grants(self, patched_seed) -> None:
        g = _make_gitea()
        db = _make_db(user=_creator())
        jid = uuid.UUID("1793b2a8-94bd-4f3c-bf4a-5ff51c9e012e")
        row = {
            "id": jid,
            "parent_job_id": None,
            "project_id": None,
            "user_id": uuid.UUID("48de2860-107f-4a8c-9a24-3a1e6c352ded"),
            "config_name": "developer",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert g.create_repo.await_args.args == ("job-1793b2a8",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        assert row["repo_name"] == "job-1793b2a8"
        db.bind_job_managed_repository.assert_awaited_once()
        assert db.bind_job_managed_repository.await_args.args[0] == str(jid)
        # Grant hardening: username/full_name/sub are passed (not email-only).
        gk = g.grant_user_repo_access.await_args
        assert gk.args[0] == "max@stud.fra-uas.de"
        assert gk.args[1] == "job-1793b2a8"
        assert gk.kwargs["username"] == "max"
        assert gk.kwargs["full_name"] == "Max Mustermann"
        assert gk.kwargs["sub"] == "d0d7dea6-6c7b-4fb2-9d47-b84096253db3"
        # No project → no cloud-baseline seed.
        patched_seed.assert_not_called()

    @pytest.mark.asyncio
    async def test_uuid_coercion_for_db_helpers(self, patched_seed) -> None:
        """``job_row`` ids are native UUIDs; the postgres_db helpers that do
        ``UUID(arg)`` must receive ``str`` or they raise."""
        g = _make_gitea()
        db = _make_db(user=_creator())
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": None,
            "user_id": uuid.uuid4(),
            "config_name": "developer",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert isinstance(db.get_user.await_args.args[0], str)
        assert isinstance(db.bind_job_managed_repository.await_args.args[0], str)

    @pytest.mark.asyncio
    async def test_project_job_ignores_legacy_shared_repo(self, patched_seed) -> None:
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[
                {"name": "project-1a387b4d-jobs", "repo_url": "http://x/y.git"}
            ],
            project={"main_cloud_folder_handle": None},
        )
        jid = uuid.UUID("abcdef12-0000-0000-0000-000000000000")
        row = {
            "id": jid,
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert g.create_repo.await_args.args == ("job-abcdef12",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        assert row["repo_name"] == "job-abcdef12"
        assert "branch_name" not in row
        g.create_branch.assert_not_called()
        db.get_project_repositories.assert_not_called()

    @pytest.mark.asyncio
    async def test_project_job_fallback_per_job_repo(self, patched_seed) -> None:
        """A project with no jobs repo falls back to a per-job repo."""
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[],
            project={"main_cloud_folder_handle": None},
        )
        jid = uuid.UUID("abcdef12-0000-0000-0000-000000000000")
        row = {
            "id": jid,
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert g.create_repo.await_args.args == ("job-abcdef12",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        assert row["repo_name"] == "job-abcdef12"
        g.create_branch.assert_not_called()

    @pytest.mark.asyncio
    async def test_subjob_branches_on_parent_repo(self, patched_seed) -> None:
        g = _make_gitea()
        parent = {
            "id": uuid.uuid4(),
            "repo_name": "job-deadbeef",
            "branch_name": "main",
            "context": {"git_remote_url": "http://x/job-deadbeef.git"},
            "parent_job_id": None,
            "project_id": None,
        }
        db = _make_db(user=_creator(), parent=parent)
        jid = uuid.UUID("abcdef12-0000-0000-0000-000000000000")
        row = {
            "id": jid,
            "parent_job_id": uuid.uuid4(),
            "project_id": None,
            "user_id": uuid.uuid4(),
            "config_name": "critic",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        g.create_branch.assert_awaited_once_with(
            "job-deadbeef", "subjob/abcdef12/critic", from_branch="main"
        )
        assert row["repo_name"] == "job-deadbeef"
        assert row["branch_name"] == "subjob/abcdef12/critic"

    @pytest.mark.asyncio
    async def test_historical_subjob_uses_exact_project_jobs_authority(
        self, patched_seed
    ) -> None:
        g = _make_gitea()
        project_id = uuid.uuid4()
        repository_id = uuid.uuid4()
        parent = {
            "id": uuid.uuid4(),
            "repo_name": None,
            "branch_name": "main",
            "context": {},
            "parent_job_id": None,
            "project_id": project_id,
        }
        jobs_repository = {
            "id": repository_id,
            "project_id": project_id,
            "name": "project-historical-jobs",
            "repo_url": "http://srw-gitea:3000/srw/project-historical-jobs.git",
            "role": "jobs",
            "read_only": False,
            "is_managed": True,
        }
        db = _make_db(parent=parent, project_repos=[jobs_repository])
        row = {
            "id": uuid.UUID("abcdef12-0000-0000-0000-000000000000"),
            "parent_job_id": uuid.uuid4(),
            "project_id": project_id,
            "user_id": None,
            "config_name": "critic",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
            require_repository=True,
        )

        g.create_branch.assert_awaited_once_with(
            "project-historical-jobs",
            "subjob/abcdef12/critic",
            from_branch="main",
        )
        assert row["repo_name"] == "project-historical-jobs"
        assert db.bind_job_managed_repository.await_args.kwargs == {
            "repo_name": "project-historical-jobs",
            "clean_url": ("http://srw-gitea:3000/srw/project-historical-jobs.git"),
        }

    @pytest.mark.asyncio
    async def test_strict_subjob_branch_failure_is_repository_preflight_failure(
        self, patched_seed
    ) -> None:
        g = _make_gitea()
        g.create_branch.return_value = False
        parent = {
            "id": uuid.uuid4(),
            "repo_name": "job-deadbeef",
            "branch_name": "main",
            "context": {"git_remote_url": "http://x/job-deadbeef.git"},
            "parent_job_id": None,
            "project_id": None,
        }
        db = _make_db(parent=parent)
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": uuid.uuid4(),
            "project_id": None,
            "user_id": None,
            "config_name": "critic",
        }

        with pytest.raises(JobProvisioningError) as exc:
            await provision_job_repo(
                job_row=row,
                gitea_client=g,
                postgres_db=db,
                main_cloud_router=MagicMock(),
                require_repository=True,
            )

        assert exc.value.phase == "repository"
        assert exc.value.failure_class == "infrastructure"
        assert "repo_name" not in row
        assert isinstance(db.get_job.await_args.args[0], str)

    @pytest.mark.asyncio
    async def test_cloud_seed_fires_when_the_job_carries_the_checkout(
        self, patched_seed
    ) -> None:
        g = _make_gitea()
        mcr = MagicMock()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={
                "name": "X",
                "main_cloud_folder_handle": "personal/Projects/X",
                "main_cloud_backend": "nextcloud",
            },
        )
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
        }
        # Admission stamped the checkout and its seeding state; create_job's
        # RETURNING row carries no context, so provisioning reads it back.
        db.get_job.return_value = {
            "context": {
                "cloud_checkout": {"datasource_id": "d", "access": "read_only"},
                "cloud_baseline": {"state": "seeding"},
                "cloud_connectors": [{"kind": "cloud_folder_checkout"}],
            }
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=mcr,
        )

        patched_seed.assert_called_once()
        kwargs = patched_seed.call_args.kwargs
        assert kwargs["repo_name"].startswith("job-")
        assert kwargs["main_cloud_router"] is mcr
        # A checkout's copy is complete: binary files too.
        assert kwargs["include_binary"] is True
        db.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_checkout_no_copy_even_with_a_folder(self, patched_seed) -> None:
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={
                "main_cloud_folder_handle": "personal/Projects/X",
                "main_cloud_backend": "nextcloud",
            },
        )
        conn = db.acquire.return_value._value
        conn.fetch = AsyncMock(return_value=[])
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
            "context": {},
        }

        await provision_job_repo(
            job_row=row, gitea_client=g, postgres_db=db, main_cloud_router=MagicMock()
        )

        patched_seed.assert_not_called()
        db.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_work_admitted_elsewhere_gets_its_attached_checkout(
        self, patched_seed
    ) -> None:
        project_id = uuid.uuid4()
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={
                "name": "My Proj",
                "main_cloud_folder_handle": "7",
                "main_cloud_backend": "nextcloud",
            },
        )
        conn = db.acquire.return_value._value
        checkout_id = str(uuid.uuid4())
        conn.fetch = AsyncMock(
            return_value=[
                {
                    "id": checkout_id,
                    "name": "Project folder copy",
                    "type": "cloud_folder_checkout",
                    "config": {"project_id": str(project_id)},
                    "managed_key": f"project-cloud-checkout:{project_id}",
                    "read_only": None,
                    "is_global": False,
                    "created_by": None,
                    "project_read_only": None,
                }
            ]
        )
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": project_id,
            "user_id": uuid.uuid4(),
            "config_name": "default",
            "context": {"automation_id": "a1"},
        }

        await provision_job_repo(
            job_row=row, gitea_client=g, postgres_db=db, main_cloud_router=MagicMock()
        )

        stamp = db.merge_job_context.await_args.args[1]
        assert stamp["cloud_checkout"] == {
            "datasource_id": checkout_id,
            "access": "reviewed_write_back",
        }
        assert stamp["cloud_baseline"] == {"state": "seeding"}
        assert stamp["cloud_connectors"] == [
            {
                "kind": "cloud_folder_checkout",
                "path": "projects/my_proj/",
                "folder": "My Proj",
                "edits": "reviewed",
                "access": "reviewed_write_back",
                "datasource_id": checkout_id,
            }
        ]
        patched_seed.assert_called_once()

    @pytest.mark.asyncio
    async def test_a_subjob_gets_no_copy_of_its_own(self, patched_seed) -> None:
        g = _make_gitea()
        parent_id = uuid.uuid4()
        db = _make_db(
            user=_creator(),
            project={
                "main_cloud_folder_handle": "7",
                "main_cloud_backend": "nextcloud",
            },
            parent={
                "id": parent_id,
                "repo_name": "job-parent",
                "branch_name": None,
                "context": {"cloud_checkout": {"access": "read_only"}},
            },
        )
        db.get_job_managed_repository_authority = AsyncMock(return_value=None)
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": parent_id,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "critic",
            "repo_name": "job-parent",
            "context": {"cloud_checkout": {"access": "read_only"}},
        }

        await provision_job_repo(
            job_row=row, gitea_client=g, postgres_db=db, main_cloud_router=MagicMock()
        )

        patched_seed.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_admitted_copy_no_seed_makes_is_released(
        self, patched_seed
    ) -> None:
        # Gitea is down: no repository, so no seed; the admitted ``seeding``
        # must not hold the job back from dispatch forever.
        g = _make_gitea(initialized=False)
        db = _make_db(user=_creator())
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
            "context": {
                "cloud_checkout": {"access": "read_only"},
                "cloud_baseline": {"state": "seeding"},
            },
        }

        await provision_job_repo(
            job_row=row, gitea_client=g, postgres_db=db, main_cloud_router=MagicMock()
        )

        patched_seed.assert_not_called()
        released = db.merge_job_context.await_args.args[1]["cloud_baseline"]
        assert released["state"] == "failed"
        assert "Gitea" in released["error"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("loop_floor", [False, True], ids=["job", "loop"])
    async def test_a_raise_before_the_seed_releases_the_admitted_copy(
        self, patched_seed, patched_loop_seed, loop_floor
    ) -> None:
        # The repository authority cannot be bound: provisioning raises after
        # create_job, before any seed. The admitted ``seeding`` would hold the
        # job in created forever; a loop's spawn settles its own failure.
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project={
                "main_cloud_folder_handle": "cloud/project",
                "main_cloud_backend": "opencloud",
            },
        )
        db.bind_job_managed_repository = AsyncMock(return_value=False)
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
            "context": {
                "cloud_checkout": {"access": "reviewed_write_back"},
                "cloud_baseline": {"state": "seeding"},
            },
        }

        with pytest.raises(JobProvisioningError):
            await provision_job_repo(
                job_row=row,
                gitea_client=g,
                postgres_db=db,
                main_cloud_router=MagicMock(),
                loop_floor=loop_floor,
            )

        patched_seed.assert_not_called()
        patched_loop_seed.assert_not_awaited()
        releases = [
            call.args[1]["cloud_baseline"]
            for call in db.merge_job_context.await_args_list
            if "cloud_baseline" in call.args[1]
        ]
        if loop_floor:
            assert releases == []
        else:
            assert releases == [
                {
                    "state": "failed",
                    "error": "the job's provisioning failed before its project "
                    "folder was copied",
                }
            ]

    @pytest.mark.asyncio
    async def test_cloud_seed_skipped_without_folder(self, patched_seed) -> None:
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={"main_cloud_folder_handle": None},
        )
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        patched_seed.assert_not_called()

    @pytest.mark.asyncio
    async def test_grant_skipped_when_repo_not_created(self, patched_seed) -> None:
        """If ``create_repo`` returns falsy, no repo_name is set → no grant,
        no crash (mirrors the original best-effort behaviour)."""
        g = _make_gitea()
        g.create_repo = AsyncMock(return_value=None)
        db = _make_db(user=_creator())
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": None,
            "user_id": uuid.uuid4(),
            "config_name": "developer",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert "repo_name" not in row
        g.grant_user_repo_access.assert_not_called()

    @pytest.mark.asyncio
    async def test_loop_job_seeds_isolated_floor_and_cloud(
        self, patched_seed, patched_loop_seed
    ) -> None:
        """A loop gets an isolated repo floor and synchronous cloud baseline."""
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[
                {"name": "project-1a387b4d-jobs", "repo_url": "http://x/y.git"}
            ],
            project={
                "main_cloud_folder_handle": "cloud/project",
                "main_cloud_backend": "opencloud",
            },
        )
        jid = uuid.UUID("abcdef12-0000-0000-0000-000000000000")
        row = {
            "id": jid,
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "developer",
        }
        db.get_job.return_value = {**row, "cloud_diff_baseline_commit": "base123"}

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
            loop_floor=True,
        )

        assert g.create_repo.await_args.args == ("job-abcdef12",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        g.create_branch.assert_not_called()
        assert row["repo_name"] == "job-abcdef12"
        assert "branch_name" not in row
        # Floor seeded: no .gitignore existed → change_files writes it to main.
        g.change_files.assert_awaited_once()
        cf = g.change_files.await_args
        assert cf.args[0] == "job-abcdef12"
        assert cf.args[1] == "main"
        assert cf.args[2][0]["path"] == ".gitignore"
        # Floor content covers job scratch + framework scaffolding. `skills/` was
        # caught leaking onto main in a k3d E2E; the agent's code dir (repo/) must
        # NOT be floored (it is the deliverable).
        import base64

        floor_lines = (
            base64.b64decode(cf.args[2][0]["content_b64"]).decode().splitlines()
        )
        assert {"todos.yaml", "archive/", "skills/", "notes/"} <= set(floor_lines)
        assert "repo/" not in floor_lines
        patched_loop_seed.assert_awaited_once()
        assert patched_loop_seed.await_args.kwargs["require_complete"] is True
        assert "authority_check" not in patched_loop_seed.await_args.kwargs

    @pytest.mark.asyncio
    async def test_loop_gitignore_floor_idempotent(
        self, patched_seed, patched_loop_seed
    ) -> None:
        """If `main` already carries the floor, it is not rewritten."""
        g = _make_gitea()
        g.get_file_bytes = AsyncMock(
            return_value=b"workspace.md\ntodos.yaml\narchive/\n"
        )
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={
                "main_cloud_folder_handle": "cloud/project",
                "main_cloud_backend": "opencloud",
            },
        )
        row = {
            "id": uuid.uuid4(),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "developer",
        }
        db.get_job.return_value = {**row, "cloud_diff_baseline_commit": "base123"}

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
            loop_floor=True,
        )

        g.change_files.assert_not_called()  # floor already present
        g.create_branch.assert_not_called()
        patched_loop_seed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_failed_post_key_provisioning_revokes_repository_authority(
        self, patched_seed, patched_loop_seed
    ) -> None:
        g = _make_gitea()
        g.get_file_bytes = AsyncMock(return_value=None)
        g.change_files = AsyncMock(return_value=False)
        db = _make_db(
            project={
                "main_cloud_folder_handle": "cloud/project",
                "main_cloud_backend": "opencloud",
            }
        )
        row = {
            "id": uuid.UUID("abcdef12-0000-0000-0000-000000000000"),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": None,
            "config_name": "developer",
        }

        with pytest.raises(JobProvisioningError):
            await provision_job_repo(
                job_row=row,
                gitea_client=g,
                postgres_db=db,
                main_cloud_router=MagicMock(),
                loop_floor=True,
            )

        # The exact job binding is already durable when the later seed fails,
        # and the cleanup path contains the repository/key before returning.
        db.bind_job_managed_repository.assert_awaited_once()
        assert row["repo_name"] == "job-abcdef12"
        g.delete_repo.assert_awaited_once_with(
            "job-abcdef12",
            intent_marker=str(
                db.claim_managed_repository_creation_cleanup.return_value[
                    "intent_marker"
                ]
            ),
        )
        patched_loop_seed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_loop_analysis_job_also_uses_isolated_cloud_baseline(
        self, patched_seed, patched_loop_seed
    ) -> None:
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={
                "main_cloud_folder_handle": "cloud/project",
                "main_cloud_backend": "opencloud",
            },
        )
        jid = uuid.UUID("abcdef12-0000-0000-0000-000000000000")
        row = {
            "id": jid,
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "scholar",
        }
        db.get_job.return_value = {**row, "cloud_diff_baseline_commit": "base123"}

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
            loop_floor=True,
        )

        assert g.create_repo.await_args.args == ("job-abcdef12",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        g.create_branch.assert_not_called()
        assert row["repo_name"] == "job-abcdef12"
        g.change_files.assert_awaited_once()  # floor seeded for analysis too
        patched_loop_seed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_loop_project_job_never_touches_floor(self, patched_seed) -> None:
        """Ordinary project jobs are isolated but do not receive loop floor."""
        g = _make_gitea()
        db = _make_db(
            user=_creator(),
            project_repos=[{"name": "p-jobs", "repo_url": "http://x/y.git"}],
            project={"main_cloud_folder_handle": None},
        )
        row = {
            "id": uuid.UUID("abcdef12-0000-0000-0000-000000000000"),
            "parent_job_id": None,
            "project_id": uuid.uuid4(),
            "user_id": uuid.uuid4(),
            "config_name": "default",
        }

        await provision_job_repo(
            job_row=row,
            gitea_client=g,
            postgres_db=db,
            main_cloud_router=MagicMock(),
        )

        assert g.create_repo.await_args.args == ("job-abcdef12",)
        assert "intent_marker" in g.create_repo.await_args.kwargs
        g.create_branch.assert_not_called()
        g.change_files.assert_not_called()


class TestIsLoopExecutionRole:
    """Execution-ness controls expected project-cloud file production."""

    def test_analysis_roles_are_not_execution(self) -> None:
        from orchestrator.services.project_loops import is_loop_execution_role

        assert is_loop_execution_role("scholar") is False
        assert is_loop_execution_role("critic") is False

    def test_product_qa_is_analysis_not_execution(self) -> None:
        # product-qa audits the shipped product and writes KB findings only —
        # it never touches repo/, so an empty merge is normal, not F29 lost work.
        # knowledge-base/knowledge/features/loop_parallel_stages.md (Phase 0).
        from orchestrator.services.project_loops import is_loop_execution_role

        assert is_loop_execution_role("product-qa") is False

    def test_execution_roles(self) -> None:
        from orchestrator.services.project_loops import is_loop_execution_role

        assert is_loop_execution_role("developer") is True
        assert is_loop_execution_role("default") is True
        assert is_loop_execution_role("writer") is True

    def test_empty_or_none_is_not_execution(self) -> None:
        from orchestrator.services.project_loops import is_loop_execution_role

        assert is_loop_execution_role(None) is False
        assert is_loop_execution_role("") is False
