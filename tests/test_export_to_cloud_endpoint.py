"""Tests for the Mode B "Open cloud folder" export flow (job_cloud_export.md).

Covers the two behaviours added when the export button moved from
``!project_id`` to "project has no cloud folder", and the export started
copying the agent's declared deliverables instead of ``output/`` wholesale:

* ``_with_cloud_review_mode`` — the pure serialization helper that turns the
  ``project_has_cloud_folder`` join column into the DTO's ``cloud_review_mode``.
* ``export_job_to_shared_folder`` — the real endpoint, driven with patched
  globals + the ``FakeMainCloudBackend``: the routing gate (project-with-cloud
  -folder → 409; default-project / loose → allowed), the deliverables copy
  (declared paths only, missing skipped, shared wrapper directories collapsed
  via ``_common_dir_prefix``), the ``output/`` fallback for jobs without a
  deliverables list, folder naming/reuse, and whether the folder actually
  ended up shared with the caller.

Follows the house pattern in tests/test_job_access.py: import ``main`` (conftest
puts orchestrator/ on sys.path) and patch its module globals.
"""

import dataclasses
import json
import re
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.routers import job_review as job_review_routes
from orchestrator.services import job_export
from tests.cloud.fake import FakeMainCloudBackend
from orchestrator.application import jobs as jobs_composition
from orchestrator.application import workspace as workspace_composition


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


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
        "id": "682baab8-7864-4f3b-9298-4134294f703e",
        "status": "pending_review",
        "project_id": None,
        "project_has_cloud_folder": False,
        "exported_folder_handle": None,
        "exported_at": None,
        "freeze_data": None,
        "repo_name": "job-682baab8",
        "description": "Write the quarterly report",
    }
    job.update(over)
    return job


def _patch_endpoint(*, user, job, backend, gitea_files, repo=("job-682baab8", "main")):
    """Patch every global ``export_job_to_shared_folder`` touches.

    ``gitea_files`` maps repo-relative path -> bytes; it backs both
    ``get_file_bytes`` (deliverables path) and ``list_contents`` (output/
    fallback). Returns (ExitStack, backend, db_mock).

    ``user``/``job`` are the pair the caller hands to ``_deps`` — the access
    gate lives on the route dependencies, not on a patchable global.
    """
    stack = ExitStack()

    router = MagicMock()
    router.for_owner = MagicMock(return_value=backend)
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.main_cloud_router", router)
    )

    gitea = MagicMock()
    gitea.is_initialized = True

    async def _get_file_bytes(repo_name, path, ref=None):
        return gitea_files.get(path)

    gitea.get_file_bytes = AsyncMock(side_effect=_get_file_bytes)

    async def _list_contents(repo_name, src_dir, ref=None):
        # Immediate children of src_dir, derived from gitea_files (one level).
        prefix = src_dir.rstrip("/") + "/"
        out: list[dict] = []
        seen_dirs: set[str] = set()
        for p in gitea_files:
            if not p.startswith(prefix):
                continue
            rest = p[len(prefix) :]
            if "/" in rest:
                d = prefix + rest.split("/")[0]
                if d not in seen_dirs:
                    seen_dirs.add(d)
                    out.append({"path": d, "type": "dir"})
            else:
                out.append({"path": p, "type": "file"})
        return out

    gitea.list_contents = AsyncMock(side_effect=_list_contents)
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.gitea_client", gitea)
    )

    stack.enter_context(
        patch(
            "orchestrator.services.subjob_output.resolve_job_repo",
            AsyncMock(return_value=repo),
        )
    )

    db = MagicMock()
    db.update_job_exported_folder = AsyncMock(return_value=True)
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))

    return stack, backend, db


def _deps(user: dict, job: dict):
    """Route dependencies built from the patched globals, auth short-circuited.

    ``require_job_access`` is a field default on ``JobReviewDependencies``, so
    the gate is replaced here rather than patched on a module the router never
    reads. Call inside the ``_patch_endpoint`` stack — the factory reads
    ``main``'s globals when it runs.
    """
    return dataclasses.replace(
        workspace_composition.job_review_dependencies(
            orchestrator.main.app.state.resources
        ),
        require_job_access=AsyncMock(return_value=(user, job)),
    )


def _copied_paths(backend: FakeMainCloudBackend) -> set[str]:
    """Relative paths PUT into the session folder by the fake backend."""
    return {rel for (_native, rel) in backend._session_files}


# --------------------------------------------------------------------------- #
# _with_cloud_review_mode
# --------------------------------------------------------------------------- #


class TestCloudReviewMode:
    def test_open_folder_when_no_cloud_folder(self):
        out = jobs_composition.with_cloud_review_mode(
            orchestrator.main.app.state.resources,
            {"id": "x", "project_has_cloud_folder": False},
        )
        assert out["cloud_review_mode"] == "open_folder"
        assert "project_has_cloud_folder" not in out

    def test_diff_when_project_has_cloud_folder(self):
        out = jobs_composition.with_cloud_review_mode(
            orchestrator.main.app.state.resources,
            {"id": "x", "project_has_cloud_folder": True},
        )
        assert out["cloud_review_mode"] == "diff"
        assert "project_has_cloud_folder" not in out

    def test_open_folder_when_column_absent(self):
        # Loose jobs / rows without the join column default to open_folder.
        out = jobs_composition.with_cloud_review_mode(
            orchestrator.main.app.state.resources, {"id": "x"}
        )
        assert out["cloud_review_mode"] == "open_folder"

    def test_does_not_mutate_input(self):
        src = {"id": "x", "project_has_cloud_folder": True}
        jobs_composition.with_cloud_review_mode(
            orchestrator.main.app.state.resources, src
        )
        assert src["project_has_cloud_folder"] is True
        assert "cloud_review_mode" not in src


# --------------------------------------------------------------------------- #
# Routing gate
# --------------------------------------------------------------------------- #


class TestExportRoutingGate:
    @pytest.mark.asyncio
    async def test_rejects_project_with_cloud_folder(self, fake_request):
        user = _make_user()
        job = _make_job(project_id="p1", project_has_cloud_folder=True)
        stack, *_ = _patch_endpoint(
            user=user, job=job, backend=FakeMainCloudBackend(), gitea_files={}
        )
        with stack, pytest.raises(HTTPException) as ei:
            await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 409
        assert "diff-review" in ei.value.detail

    @pytest.mark.asyncio
    async def test_rejects_wrong_status(self, fake_request):
        user = _make_user()
        job = _make_job(status="processing")
        stack, *_ = _patch_endpoint(
            user=user, job=job, backend=FakeMainCloudBackend(), gitea_files={}
        )
        with stack, pytest.raises(HTTPException) as ei:
            await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert ei.value.status_code == 409

    @pytest.mark.asyncio
    async def test_allows_default_project_without_cloud_folder(self, fake_request):
        # The dead-zone case this change fixes: project_id is set (the
        # auto-assigned default project) but the project has no cloud folder.
        user = _make_user()
        job = _make_job(
            status="pending_review",
            project_id="default-proj",
            project_has_cloud_folder=False,
            freeze_data={"deliverables": ["spec.yaml"]},
        )
        stack, backend, db = _patch_endpoint(
            user=user,
            job=job,
            backend=FakeMainCloudBackend(),
            gitea_files={"spec.yaml": b"feature: x\n"},
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1
        assert db.update_job_exported_folder.await_count == 1

    @pytest.mark.asyncio
    async def test_allows_pending_review_status(self, fake_request):
        # In-review export = preview before approving.
        user = _make_user()
        job = _make_job(
            status="pending_review", freeze_data={"deliverables": ["spec.yaml"]}
        )
        stack, backend, _ = _patch_endpoint(
            user=user,
            job=job,
            backend=FakeMainCloudBackend(),
            gitea_files={"spec.yaml": b"x"},
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1


# --------------------------------------------------------------------------- #
# Deliverables copy
# --------------------------------------------------------------------------- #


class TestDeliverablesCopy:
    @pytest.mark.asyncio
    async def test_copies_declared_deliverables_preserving_paths(self, fake_request):
        user = _make_user()
        deliverables = [
            "spec.yaml",
            "spec_lock.md",
            "repo/src/app.py",
            "repo/tests/test_app.py",
            "repo/requirements.txt",
        ]
        job = _make_job(freeze_data={"deliverables": deliverables})
        gitea_files = {p: f"content of {p}".encode() for p in deliverables}
        # Undeclared scaffolding / metadata that lives in the repo but must NOT
        # be exported (this is what the old output/-only copy would have sent).
        gitea_files["output/job_frozen.json"] = b"{}"
        gitea_files["tools/x.py"] = b"scaffold"

        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files=gitea_files
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )

        assert result["files_copied"] == 5
        copied = _copied_paths(backend)
        assert copied == set(deliverables)
        assert "output/job_frozen.json" not in copied
        assert "tools/x.py" not in copied
        # repo/ structure preserved verbatim.
        assert "repo/src/app.py" in copied

    @pytest.mark.asyncio
    async def test_skips_declared_deliverable_missing_in_repo(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"deliverables": ["output/report.md", "gone.txt"]})
        gitea_files = {"output/report.md": b"# report"}  # gone.txt absent
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files=gitea_files
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1
        # `output/` is NOT collapsed here: the prefix is computed over the
        # declared set, and root-level `gone.txt` is in it even though it turned
        # out to be missing. Degrades to less collapsing, never to wrong paths.
        assert _copied_paths(backend) == {"output/report.md"}

    @pytest.mark.asyncio
    async def test_rejects_path_escape(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"deliverables": ["../secret.txt", "ok.md"]})
        gitea_files = {"ok.md": b"ok", "../secret.txt": b"nope"}
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files=gitea_files
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1
        assert _copied_paths(backend) == {"ok.md"}

    @pytest.mark.asyncio
    async def test_falls_back_to_output_tree_when_no_deliverables(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"summary": "done"})  # no deliverables key
        gitea_files = {"output/a.md": b"a", "output/sub/b.md": b"b"}
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files=gitea_files
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 2
        # `output/` is the shared wrapper and gets collapsed; `sub/` survives
        # because it distinguishes b.md from a.md.
        assert _copied_paths(backend) == {"a.md", "sub/b.md"}

    @pytest.mark.asyncio
    async def test_freeze_data_as_json_string(self, fake_request):
        # asyncpg may hand back JSONB as a str — the endpoint must json.loads it.
        user = _make_user()
        job = _make_job(freeze_data=json.dumps({"deliverables": ["spec.yaml"]}))
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files={"spec.yaml": b"x"}
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1
        assert _copied_paths(backend) == {"spec.yaml"}


# --------------------------------------------------------------------------- #
# Sharing
# --------------------------------------------------------------------------- #


class TestExportSharing:
    """The export is only *useful* if the folder ends up visible to the caller.

    Backends that can't provision accounts (Nextcloud: user_oidc materialises
    one on first browser login) resolve to None until the user has signed in
    once, and the endpoint then copies files into a folder nobody but the agent
    can see. That must be reported, not swallowed.
    """

    @pytest.mark.asyncio
    async def test_shares_with_resolved_user(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"deliverables": ["spec.yaml"]})
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files={"spec.yaml": b"x"}
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["shared"] is True
        assert [c[0] for c in backend.calls].count("share_session_folder") == 1

    @pytest.mark.asyncio
    async def test_reports_unshared_when_no_cloud_account(self, fake_request):
        # No email and no display name = nothing the backend can resolve an
        # account by, which is what an un-provisioned user looks like.
        user = {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "keycloak_sub": "kc-1"}
        job = _make_job(freeze_data={"deliverables": ["spec.yaml"]})
        backend = FakeMainCloudBackend()
        stack, backend, db = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files={"spec.yaml": b"x"}
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["shared"] is False
        assert "share_session_folder" not in [c[0] for c in backend.calls]
        # Still a real export: files copied and the job stamped, so a re-sync
        # after the user's first cloud login shares the same folder.
        assert result["files_copied"] == 1
        assert _copied_paths(backend) == {"spec.yaml"}
        assert db.update_job_exported_folder.await_count == 1


# --------------------------------------------------------------------------- #
# Wrapper-directory collapse
# --------------------------------------------------------------------------- #


class TestCommonDirPrefix:
    def test_single_file_in_a_directory(self):
        assert job_export._common_dir_prefix(["output/digest.md"]) == "output"

    def test_single_file_at_root(self):
        assert job_export._common_dir_prefix(["done.txt"]) == ""

    def test_shared_head_only(self):
        assert (
            job_export._common_dir_prefix(["repo/src/a.py", "repo/tests/b.py"])
            == "repo"
        )

    def test_nothing_shared(self):
        assert job_export._common_dir_prefix(["spec.yaml", "repo/a.py"]) == ""

    def test_whole_shared_path(self):
        assert (
            job_export._common_dir_prefix(["out/deep/a.md", "out/deep/b.md"])
            == "out/deep"
        )

    def test_matches_whole_segments_not_string_prefixes(self):
        # "out" is a string prefix of "output" but a different directory.
        assert job_export._common_dir_prefix(["out/a.md", "output/b.md"]) == ""

    def test_empty(self):
        assert job_export._common_dir_prefix([]) == ""


class TestWrapperCollapse:
    """A folder holding one `output/` holding one file is two clicks to nothing.
    The shared leading directories come off; anything that distinguishes files
    stays."""

    @pytest.mark.asyncio
    async def test_collapses_lone_output_wrapper(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"deliverables": ["output/digest.md"]})
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user,
            job=job,
            backend=backend,
            gitea_files={"output/digest.md": b"# digest"},
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["files_copied"] == 1
        assert _copied_paths(backend) == {"digest.md"}

    @pytest.mark.asyncio
    async def test_collapses_deep_wrapper_for_a_lone_file(self, fake_request):
        user = _make_user()
        job = _make_job(freeze_data={"deliverables": ["output/reports/q1.md"]})
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user,
            job=job,
            backend=backend,
            gitea_files={"output/reports/q1.md": b"q1"},
        )
        with stack:
            await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert _copied_paths(backend) == {"q1.md"}

    @pytest.mark.asyncio
    async def test_keeps_structure_that_distinguishes_files(self, fake_request):
        user = _make_user()
        deliverables = ["repo/src/app.py", "repo/tests/test_app.py"]
        job = _make_job(freeze_data={"deliverables": deliverables})
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user,
            job=job,
            backend=backend,
            gitea_files={p: b"x" for p in deliverables},
        )
        with stack:
            await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        # Only the shared `repo/` head comes off.
        assert _copied_paths(backend) == {"src/app.py", "tests/test_app.py"}

    @pytest.mark.asyncio
    async def test_root_level_deliverable_pins_everything_in_place(self, fake_request):
        user = _make_user()
        deliverables = ["spec.yaml", "output/report.md"]
        job = _make_job(freeze_data={"deliverables": deliverables})
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user,
            job=job,
            backend=backend,
            gitea_files={p: b"x" for p in deliverables},
        )
        with stack:
            await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert _copied_paths(backend) == {"spec.yaml", "output/report.md"}


# --------------------------------------------------------------------------- #
# Folder naming
# --------------------------------------------------------------------------- #


class TestExportFolderName:
    """Names must be readable (a cloud root of `job-<uuid>` is unnavigable)
    AND deterministic (the endpoint re-derives the name to find the folder on
    a re-sync)."""

    JOB = "a6fa6f2a-9101-41f4-9ccb-5a7f362dc305"

    def test_slugs_the_description_and_keeps_an_id_suffix(self):
        assert (
            job_export._job_export_folder_name(self.JOB, "You maintain a daily digest.")
            == "you-maintain-a-daily-digest-a6fa6f2a"
        )

    def test_deterministic(self):
        a = job_export._job_export_folder_name(self.JOB, "Same prompt")
        b = job_export._job_export_folder_name(self.JOB, "Same prompt")
        assert a == b

    def test_same_description_different_jobs_do_not_collide(self):
        other = "11111111-2222-3333-4444-555555555555"
        assert job_export._job_export_folder_name(
            self.JOB, "Shared prompt"
        ) != job_export._job_export_folder_name(other, "Shared prompt")

    def test_truncates_on_a_word_boundary(self):
        name = job_export._job_export_folder_name(
            self.JOB,
            "Implement a small Python CLI tool in this workspace, test first",
        )
        slug = name.rsplit("-", 1)[0]
        assert len(slug) <= 40
        # No half-word at the end.
        assert slug == "implement-a-small-python-cli-tool-in"

    def test_only_uses_the_first_line(self):
        name = job_export._job_export_folder_name(self.JOB, "Daily digest\nSecond line")
        assert name == "daily-digest-a6fa6f2a"

    def test_folds_accents_rather_than_dropping_them(self):
        assert job_export._job_export_folder_name(
            self.JOB, "Führe die Prüfung durch"
        ) == ("fuhre-die-prufung-durch-a6fa6f2a")

    def test_path_safe_charset(self):
        name = job_export._job_export_folder_name(
            self.JOB, "../../etc/passwd & <script> 100% done"
        )
        assert re.fullmatch(r"[a-z0-9-]+", name), name

    def test_falls_back_when_nothing_slugs(self):
        for desc in ("", "   ", "!!! ???", None):
            assert (
                job_export._job_export_folder_name(self.JOB, desc) == "job-a6fa6f2a9101"
            )


class TestExportFolderReuse:
    @pytest.mark.asyncio
    async def test_resync_reuses_the_existing_folder(self, fake_request):
        """A job exported under the old `job-<uuid>` scheme must re-sync into
        that same folder — re-deriving would strand it and its share."""
        user = _make_user()
        job = _make_job(
            exported_folder_handle="sessions/job-682baab87864",
            exported_at="2026-08-04T08:19:36Z",
            freeze_data={"deliverables": ["spec.yaml"]},
        )
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files={"spec.yaml": b"x"}
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["folder"]["name"] == "job-682baab87864"
        assert result["folder"]["path"] == "/job-682baab87864"

    @pytest.mark.asyncio
    async def test_first_export_uses_the_slugged_name(self, fake_request):
        user = _make_user()
        job = _make_job(
            description="Write the quarterly report",
            freeze_data={"deliverables": ["spec.yaml"]},
        )
        backend = FakeMainCloudBackend()
        stack, backend, _ = _patch_endpoint(
            user=user, job=job, backend=backend, gitea_files={"spec.yaml": b"x"}
        )
        with stack:
            result = await job_review_routes.export_job_to_shared_folder(
                fake_request, job["id"], dependencies=_deps(user, job)
            )
        assert result["folder"]["name"] == "write-the-quarterly-report-682baab8"
        assert result["folder"]["path"] == "/write-the-quarterly-report-682baab8"


# --------------------------------------------------------------------------- #
# exported_folder_url resolution
# --------------------------------------------------------------------------- #


class TestExportedFolderUrl:
    """The stored handle is opaque, so the cockpit's "Open cloud folder" button
    depends on the orchestrator resolving it into a URL on every job read —
    the export response alone can't carry it across a page reload."""

    def _with_backend(self, backend):
        router = MagicMock()
        router.for_backend = MagicMock(return_value=backend)
        return patch("orchestrator.main.app.state.resources.main_cloud_router", router)

    def test_resolves_handle_to_url(self):
        with self._with_backend(FakeMainCloudBackend()):
            out = jobs_composition.with_cloud_review_mode(
                orchestrator.main.app.state.resources,
                {"id": "x", "exported_folder_handle": "sessions/job-abc"},
            )
        assert out["exported_folder_url"] == "fake://session/sessions/job-abc"

    def test_null_when_never_exported(self):
        with self._with_backend(FakeMainCloudBackend()):
            out = jobs_composition.with_cloud_review_mode(
                orchestrator.main.app.state.resources,
                {"id": "x", "exported_folder_handle": None},
            )
        assert out["exported_folder_url"] is None

    def test_null_when_backend_down(self):
        # Cloud outage must degrade to "no button", not a broken link.
        with self._with_backend(FakeMainCloudBackend(start_initialized=False)):
            out = jobs_composition.with_cloud_review_mode(
                orchestrator.main.app.state.resources,
                {"id": "x", "exported_folder_handle": "sessions/job-abc"},
            )
        assert out["exported_folder_url"] is None
