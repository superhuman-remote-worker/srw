"""R1.B04 lane C — the Mode A job diff review routes.

Characterization first: ``tests/test_job_diff_endpoints.py`` already pins the
two read routes, and ``tests/test_completion_class_a.py`` pins one Class-A
statement per accept/reject, but nothing pinned the *ordering* around
completion control — that the guard fires before every gate, that the claim is
taken only after the last cheap refusal, and that every failure between the
claim and the commit releases it. Those, plus each refusal's exact status and
``detail`` shape (string vs dict), are written down here before the move is
relied on.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.routers.job_diff import (
    JobDiffDependencies,
    accept_job_diff,
    get_job_diff,
    get_job_diff_file,
    reject_job_diff,
    router as job_diff_router,
)
from orchestrator.services import job_diff_review
from tests._mounted_router import mount_router
from tests._route_inventory import mounted_route_objects


JOB_ID = "a6fa6f2a-9101-4c1e-9b9e-0000000000c1"
PROJECT_ID = "b6fa6f2a-9101-4c1e-9b9e-0000000000c2"
USER = {"id": "c6fa6f2a-9101-4c1e-9b9e-0000000000c3"}
BASELINE = "a" * 40
HEAD = "b" * 40


def _job(**over):
    job = {
        "id": JOB_ID,
        "status": "pending_review",
        "project_id": PROJECT_ID,
        "diff_status": "pending",
        "cloud_diff_baseline_commit": BASELINE,
        "repo_name": "job-a6fa6f2a",
        "execution_lane": "pinned",
        "context": {},
    }
    job.update(over)
    return job


def _project(**over):
    project = {
        "id": PROJECT_ID,
        "main_cloud_backend": "nextcloud",
        "main_cloud_folder_handle": "opaque-handle",
    }
    project.update(over)
    return project


class _Store:
    """Records the order of every write the no-claim path makes."""

    def __init__(self, project=None, *, role="editor"):
        self.calls: list[tuple[str, tuple, dict]] = []
        self._project = project
        self.role = role
        self.role_lookups: list[tuple[str, str]] = []

    async def get_project(self, project_id):
        self.calls.append(("get_project", (project_id,), {}))
        return self._project

    async def get_user_role_in_project(self, project_id, user_id):
        # Not a write: kept out of ``calls``, whose order the tests pin.
        self.role_lookups.append((project_id, user_id))
        return self.role

    async def update_job_cloud_diff(self, job_id, **kw):
        self.calls.append(("update_job_cloud_diff", (job_id,), kw))

    async def update_job_merge_status(self, job_id, **kw):
        self.calls.append(("update_job_merge_status", (job_id,), kw))

    async def update_job_status(self, job_id, **kw):
        self.calls.append(("update_job_status", (job_id,), kw))

    async def merge_job_context(self, job_id, patch_):
        self.calls.append(("merge_job_context", (job_id,), {"patch": patch_}))

    @property
    def names(self):
        return [name for name, _args, _kw in self.calls]


class _Control:
    """A recording stand-in for the B08 completion-control authority."""

    def __init__(self, *, claim=None, guard_error=None, finish_rows=(None,)):
        self.order: list[str] = []
        self.claim_value = claim
        self.guard_error = guard_error
        self.aborted: list = []
        self.finish_rows = list(finish_rows)
        self.statements: list[tuple[str, tuple]] = []

    async def guard(self, job_id, *, source):
        self.order.append(f"guard:{source}")
        if self.guard_error is not None:
            raise self.guard_error

    async def claim(self, job, *, source):
        self.order.append(f"claim:{source}")
        assert job["id"] == JOB_ID
        return self.claim_value

    async def abort(self, claim):
        self.order.append("abort")
        self.aborted.append(claim)

    def get(self):
        self.order.append("get_control")
        control = MagicMock()
        control.finish_claim = self._finish_claim
        return control

    @asynccontextmanager
    async def _finish_claim(self, claim):
        self.order.append("finish_claim")
        row = self.finish_rows.pop(0) if self.finish_rows else None

        async def fetchrow(sql, *args):
            self.statements.append((sql, args))
            return row

        yield SimpleNamespace(fetchrow=fetchrow), {"id": JOB_ID}


def _forge(initialized=True):
    return SimpleNamespace(is_initialized=initialized, name="forge")


def _ops(*, store=None, control=None, cloud_router=None, forge=None, advance=None):
    control = control or _Control()
    return job_diff_review.JobDiffReviewDependencies(
        store=store if store is not None else _Store(_project()),
        vector_store=SimpleNamespace(name="vector"),
        forge=forge if forge is not None else _forge(),
        cloud_router=cloud_router
        or SimpleNamespace(for_project=lambda _p: SimpleNamespace(is_initialized=True)),
        get_completion_control=control.get,
        guard_completion_control=control.guard,
        claim_completion_control=control.claim,
        abort_completion_control_claim=control.abort,
        advance_project_loop=advance or AsyncMock(),
    ), control


def _deps(job=None, *, user=None, **kw):
    ops, control = _ops(**kw)
    route_deps = JobDiffDependencies(
        store=SimpleNamespace(name="auth-store"),
        diff_review=ops,
        require_job_access=AsyncMock(
            return_value=(user or USER, job if job else _job())
        ),
    )
    return route_deps, control


def _request():
    return SimpleNamespace(headers={})


def _summary(files=(("projects/a/x.md", "modified"),), meta=None):
    return SimpleNamespace(
        meta=meta or {"baseline_commit": BASELINE, "head_commit": HEAD},
        files=[SimpleNamespace(path=p, status=s) for p, s in files],
    )


def _diff_source(summary=None, file=None):
    source = MagicMock()
    source.summary = AsyncMock(return_value=summary)
    source.file = AsyncMock(return_value=file)
    factory = MagicMock(return_value=source)
    return factory, source


# =============================================================================
# GET /api/jobs/{job_id}/diff
# =============================================================================


class TestGetJobDiff:
    @pytest.mark.asyncio
    async def test_no_baseline_is_a_404_before_the_forge_check(self):
        deps, _ = _deps(job=_job(cloud_diff_baseline_commit=None), forge=_forge(False))

        with pytest.raises(HTTPException) as exc:
            await get_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == "Job has no Mode A diff baseline."

    @pytest.mark.asyncio
    async def test_an_uninitialized_forge_is_a_503(self):
        deps, _ = _deps(forge=_forge(False))

        with pytest.raises(HTTPException) as exc:
            await get_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        assert exc.value.detail == "Gitea not available."

    @pytest.mark.asyncio
    async def test_a_summary_of_none_is_a_404_naming_the_cause(self):
        deps, _ = _deps()
        factory, _src = _diff_source(summary=None)

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            with pytest.raises(HTTPException) as exc:
                await get_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == "Diff unavailable (no repo or no head)."

    @pytest.mark.asyncio
    async def test_the_summary_projection_carries_only_path_and_status(self):
        deps, _ = _deps()
        factory, _src = _diff_source(summary=_summary())

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            result = await get_job_diff(_request(), JOB_ID, dependencies=deps)

        assert result == {
            "job_id": JOB_ID,
            "diff_status": "pending",
            "baseline_commit": BASELINE,
            "head_commit": HEAD,
            "files": [{"path": "projects/a/x.md", "status": "modified"}],
        }
        assert factory.call_args.kwargs["gitea_client"] is deps.diff_review.forge


# =============================================================================
# GET /api/jobs/{job_id}/diff/{file_path:path}
# =============================================================================


class TestGetJobDiffFile:
    @pytest.mark.asyncio
    async def test_refusal_order_is_baseline_then_scope_then_forge_then_repo(self):
        # No baseline wins even for an out-of-scope path on an offline forge.
        deps, _ = _deps(
            job=_job(cloud_diff_baseline_commit=None, repo_name=None),
            forge=_forge(False),
        )
        with pytest.raises(HTTPException) as exc:
            await get_job_diff_file(_request(), JOB_ID, "etc/passwd", dependencies=deps)
        assert (exc.value.status_code, exc.value.detail) == (
            404,
            "Job has no Mode A diff baseline.",
        )

        # With a baseline, the scope check wins over the offline forge.
        deps, _ = _deps(job=_job(repo_name=None), forge=_forge(False))
        with pytest.raises(HTTPException) as exc:
            await get_job_diff_file(_request(), JOB_ID, "etc/passwd", dependencies=deps)
        assert (exc.value.status_code, exc.value.detail) == (
            400,
            "Per-file diff is scoped to projects/<slug>/* paths.",
        )

        # In scope, the offline forge wins over the missing repo name.
        deps, _ = _deps(job=_job(repo_name=None), forge=_forge(False))
        with pytest.raises(HTTPException) as exc:
            await get_job_diff_file(
                _request(), JOB_ID, "projects/a/x.md", dependencies=deps
            )
        assert (exc.value.status_code, exc.value.detail) == (
            503,
            "Gitea not available.",
        )

        # Everything else in place, a job with no repo is a 404.
        deps, _ = _deps(job=_job(repo_name=None))
        with pytest.raises(HTTPException) as exc:
            await get_job_diff_file(
                _request(), JOB_ID, "projects/a/x.md", dependencies=deps
            )
        assert (exc.value.status_code, exc.value.detail) == (
            404,
            "Job repo not found.",
        )

    @pytest.mark.asyncio
    async def test_a_path_outside_the_diff_is_a_404_naming_the_path(self):
        deps, _ = _deps()
        factory, _src = _diff_source(summary=_summary())

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            with pytest.raises(HTTPException) as exc:
                await get_job_diff_file(
                    _request(), JOB_ID, "projects/a/other.md", dependencies=deps
                )

        assert exc.value.status_code == 404
        assert exc.value.detail == "Path 'projects/a/other.md' is not in the diff."

    @pytest.mark.asyncio
    async def test_a_listed_path_whose_content_is_absent_is_the_same_404(self):
        deps, _ = _deps()
        factory, _src = _diff_source(summary=_summary(), file=None)

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            with pytest.raises(HTTPException) as exc:
                await get_job_diff_file(
                    _request(), JOB_ID, "projects/a/x.md", dependencies=deps
                )

        assert exc.value.status_code == 404
        assert exc.value.detail == "Path 'projects/a/x.md' is not in the diff."

    @pytest.mark.asyncio
    async def test_both_sides_of_the_content_are_returned(self):
        deps, _ = _deps()
        content = SimpleNamespace(
            status="modified", old_content="one", new_content="two"
        )
        factory, _src = _diff_source(summary=_summary(), file=content)

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            result = await get_job_diff_file(
                _request(), JOB_ID, "projects/a/x.md", dependencies=deps
            )

        assert result == {
            "job_id": JOB_ID,
            "path": "projects/a/x.md",
            "status": "modified",
            "old_content": "one",
            "new_content": "two",
        }


# =============================================================================
# POST /api/jobs/{job_id}/accept
# =============================================================================


@asynccontextmanager
async def _baseline_patches(
    *, summary=None, diverged=None, apply_result=None, loop_id=None
):
    with (
        patch(
            "orchestrator.services.job_cloud_baseline.get_diff_summary",
            AsyncMock(
                return_value=summary
                if summary is not None
                else {"files": [], "head_commit": HEAD}
            ),
        ) as get_summary,
        patch(
            "orchestrator.services.job_cloud_baseline.detect_external_mods",
            AsyncMock(return_value=diverged or []),
        ) as detect,
        patch(
            "orchestrator.services.job_cloud_baseline.apply_diff_to_cloud",
            AsyncMock(
                return_value=apply_result
                if apply_result is not None
                else {"applied": 2, "deleted": 1, "errors": []}
            ),
        ) as apply_,
        patch(
            "orchestrator.services.job_cloud_baseline.project_folder_slug",
            MagicMock(return_value="alpha"),
        ),
        patch(
            "orchestrator.services.project_loops.job_loop_id",
            MagicMock(return_value=loop_id),
        ),
        patch(
            "orchestrator.services.completion.apply_terminal_job_side_effects",
            AsyncMock(return_value={"actions": ["tagged"]}),
        ) as side_effects,
    ):
        yield SimpleNamespace(
            get_summary=get_summary,
            detect=detect,
            apply=apply_,
            side_effects=side_effects,
        )


class TestAcceptJobDiffGates:
    @pytest.mark.asyncio
    async def test_the_guard_fires_before_every_gate(self):
        control = _Control(guard_error=HTTPException(409, "completion finalizing"))
        store = _Store(_project())
        deps, _ = _deps(job=_job(status="completed"), control=control, store=store)

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == "completion finalizing"
        # The status gate would also have refused; the guard got there first.
        assert control.order == ["guard:mode_a_accept"]
        assert store.calls == []

    @pytest.mark.parametrize(
        ("job_over", "detail"),
        [
            (
                {"status": "processing"},
                "Job is in status 'processing'; "
                "only pending_review jobs can be accepted.",
            ),
            (
                {"project_id": None},
                "Job has no project attached; nothing to write back to.",
            ),
            (
                {"diff_status": "accepted"},
                "Job diff_status is 'accepted'; only pending diffs can be accepted.",
            ),
            (
                {"cloud_diff_baseline_commit": None},
                "Job has no Mode A baseline; nothing to compare against.",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_each_cheap_gate_is_a_409_string_and_takes_no_claim(
        self, job_over, detail
    ):
        deps, control = _deps(job=_job(**job_over))

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == detail
        assert control.order == ["guard:mode_a_accept"]

    @pytest.mark.asyncio
    async def test_an_offline_forge_is_a_503_before_the_project_read(self):
        store = _Store(_project())
        deps, control = _deps(forge=_forge(False), store=store)

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        assert exc.value.detail == "Gitea not available."
        assert store.calls == []
        assert control.order == ["guard:mode_a_accept"]

    @pytest.mark.asyncio
    async def test_a_missing_project_is_a_404(self):
        deps, control = _deps(store=_Store(None))

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 404
        assert exc.value.detail == "Project not found."
        assert "claim:mode_a_accept" not in control.order

    @pytest.mark.parametrize(
        ("project_over", "detail"),
        [
            (
                {"main_cloud_folder_handle": None},
                "Project has no cloud folder; cannot apply diff.",
            ),
            (
                {"main_cloud_backend": None},
                "Project has no cloud backend; cannot apply diff.",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_project_without_a_cloud_target_is_a_409(
        self, project_over, detail
    ):
        deps, control = _deps(store=_Store(_project(**project_over)))

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == detail
        assert control.order == ["guard:mode_a_accept"]

    @pytest.mark.asyncio
    async def test_an_unroutable_backend_is_a_503_naming_the_backend(self):
        def boom(_project):
            raise RuntimeError("no such instance")

        deps, control = _deps(cloud_router=SimpleNamespace(for_project=boom))

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        # The router's exception text is logged, not returned; the client gets
        # the backend name it needs plus the id that finds the log line.
        assert "no such instance" not in exc.value.detail
        assert re.fullmatch(
            r"Cloud backend 'nextcloud' unavailable \(error_ref=[0-9a-f]{12}\)",
            exc.value.detail,
        )
        assert control.order == ["guard:mode_a_accept"]

    @pytest.mark.asyncio
    async def test_an_uninitialized_backend_is_a_503_and_takes_no_claim(self):
        deps, control = _deps(
            cloud_router=SimpleNamespace(
                for_project=lambda _p: SimpleNamespace(is_initialized=False)
            )
        )

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 503
        assert exc.value.detail == "Cloud backend not initialized."
        assert control.order == ["guard:mode_a_accept"]


class TestAcceptJobDiffClaimLifecycle:
    @pytest.mark.asyncio
    async def test_the_claim_is_taken_after_the_last_gate(self):
        deps, control = _deps(control=_Control(claim=None))

        async with _baseline_patches():
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert control.order[:2] == ["guard:mode_a_accept", "claim:mode_a_accept"]

    @pytest.mark.asyncio
    async def test_a_summary_failure_releases_the_claim_and_re_raises(self):
        claim = object()
        deps, control = _deps(control=_Control(claim=claim))

        async with _baseline_patches() as calls:
            calls.get_summary.side_effect = RuntimeError("gitea exploded")
            with pytest.raises(RuntimeError):
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert control.aborted == [claim]
        assert control.order == [
            "guard:mode_a_accept",
            "claim:mode_a_accept",
            "abort",
        ]

    @pytest.mark.asyncio
    async def test_a_detection_failure_releases_the_claim_and_re_raises(self):
        claim = object()
        deps, control = _deps(control=_Control(claim=claim))

        async with _baseline_patches() as calls:
            calls.detect.side_effect = RuntimeError("propfind exploded")
            with pytest.raises(RuntimeError):
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert control.aborted == [claim]

    @pytest.mark.asyncio
    async def test_divergence_releases_the_claim_and_409s_with_the_paths(self):
        claim = object()
        deps, control = _deps(control=_Control(claim=claim))

        async with _baseline_patches(diverged=["notes.md"]):
            with pytest.raises(HTTPException) as exc:
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == {
            "code": "external_modifications_detected",
            "message": (
                "Cloud folder was modified externally since the job "
                "started. Resolve manually before accepting."
            ),
            "diverged": ["notes.md"],
        }
        assert control.aborted == [claim]

    @pytest.mark.asyncio
    async def test_an_apply_failure_releases_the_claim_and_re_raises(self):
        claim = object()
        deps, control = _deps(control=_Control(claim=claim))

        async with _baseline_patches() as calls:
            calls.apply.side_effect = RuntimeError("webdav exploded")
            with pytest.raises(RuntimeError):
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert control.aborted == [claim]

    @pytest.mark.asyncio
    async def test_a_partial_write_releases_the_claim_and_502s_with_the_counts(self):
        claim = object()
        store = _Store(_project())
        deps, control = _deps(control=_Control(claim=claim), store=store)

        async with _baseline_patches(
            apply_result={"applied": 1, "deleted": 0, "errors": ["b.md: 507"]}
        ):
            with pytest.raises(HTTPException) as exc:
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 502
        assert exc.value.detail == {
            "code": "partial_write_failure",
            "applied": 1,
            "deleted": 0,
            "errors": ["b.md: 507"],
        }
        assert control.aborted == [claim]
        # The job is not transitioned — the user can retry.
        assert store.names == ["get_project"]

    @pytest.mark.asyncio
    async def test_a_lost_commit_race_releases_the_claim_and_409s(self):
        claim = object()
        control = _Control(claim=claim, finish_rows=[None])
        deps, _ = _deps(control=control)

        async with _baseline_patches():
            with pytest.raises(HTTPException) as exc:
                await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "job changed while Mode A accept was being committed"
        )
        assert control.aborted == [claim]
        assert control.order[-1] == "abort"

    @pytest.mark.asyncio
    async def test_the_claimed_commit_is_one_guarded_statement(self):
        control = _Control(claim=object(), finish_rows=[{"id": JOB_ID}])
        store = _Store(_project())
        deps, _ = _deps(control=control, store=store)

        async with _baseline_patches():
            result = await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert result["status"] == "completed"
        assert result["diff_status"] == "accepted"
        assert len(control.statements) == 1
        sql, args = control.statements[0]
        assert "diff_status='accepted'" in sql
        assert "merge_status='cloud-applied'" in sql
        assert "completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP)" in sql
        assert "AND execution_lane=$3::text" in sql
        assert args[0] == JOB_ID
        assert args[2] == "pinned"
        # No degraded per-column writes when the claim exists.
        assert store.names == ["get_project"]
        assert control.aborted == []


class TestAcceptJobDiffWithoutCompletionControl:
    @pytest.mark.asyncio
    async def test_the_transition_degrades_to_four_ordered_writes(self):
        store = _Store(_project())
        deps, control = _deps(control=_Control(claim=None), store=store)

        async with _baseline_patches():
            result = await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert store.names == [
            "get_project",
            "update_job_cloud_diff",
            "update_job_merge_status",
            "update_job_status",
            "merge_job_context",
        ]
        assert store.calls[1][2] == {"diff_status": "accepted"}
        assert store.calls[2][2] == {"merge_status": "cloud-applied"}
        assert store.calls[3][2] == {"status": "completed"}
        assert store.calls[4][2]["patch"]["loop_cloud_delivery"] == {
            "delivery_status": "cloud-applied",
            "needs_review": False,
            "delivery_sha": HEAD,
            "notes": [],
            "applied": 2,
            "deleted": 1,
        }
        assert "finish_claim" not in control.order
        assert result == {
            "job_id": JOB_ID,
            "diff_status": "accepted",
            "status": "completed",
            "applied": 2,
            "deleted": 1,
            "actions": ["tagged"],
        }


class TestAcceptJobDiffEffects:
    @pytest.mark.asyncio
    async def test_the_in_memory_row_is_updated_before_the_terminal_effects(self):
        job = _job()
        deps, _ = _deps(job=job, control=_Control(claim=None))

        async with _baseline_patches() as calls:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert job["status"] == "completed"
        assert job["diff_status"] == "accepted"
        assert job["merge_status"] == "cloud-applied"
        assert job["context"]["loop_cloud_delivery"]["delivery_sha"] == HEAD
        effect_job = calls.side_effects.await_args.args[0]
        assert effect_job is job
        assert effect_job["status"] == "completed"

    @pytest.mark.asyncio
    async def test_a_string_context_is_parsed_before_the_delivery_is_merged(self):
        job = _job(context='{"seen": true}')
        deps, _ = _deps(job=job, control=_Control(claim=None))

        async with _baseline_patches():
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert job["context"]["seen"] is True
        assert "loop_cloud_delivery" in job["context"]

    @pytest.mark.asyncio
    async def test_an_unparseable_context_is_replaced_not_raised(self):
        job = _job(context="{not json")
        deps, _ = _deps(job=job, control=_Control(claim=None))

        async with _baseline_patches():
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert list(job["context"]) == ["loop_cloud_delivery"]

    @pytest.mark.asyncio
    async def test_a_loop_job_advances_the_loop_instead_of_the_terminal_effects(self):
        advance = AsyncMock()
        deps, _ = _deps(control=_Control(claim=None), advance=advance)

        async with _baseline_patches(loop_id="loop-1") as calls:
            result = await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        advance.assert_awaited_once()
        calls.side_effects.assert_not_awaited()
        assert result["actions"] == []

    @pytest.mark.asyncio
    async def test_the_terminal_effects_get_the_forge_store_and_vector_store(self):
        deps, _ = _deps(control=_Control(claim=None))

        async with _baseline_patches() as calls:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        kwargs = calls.side_effects.await_args.kwargs
        assert kwargs["gitea"] is deps.diff_review.forge
        assert kwargs["db"] is deps.diff_review.store
        assert kwargs["vector_db"] is deps.diff_review.vector_store
        assert calls.side_effects.await_args.args[1] == "completed"

    @pytest.mark.asyncio
    async def test_only_the_projects_prefix_narrows_the_divergence_scope(self):
        deps, _ = _deps(control=_Control(claim=None))
        summary = {
            "head_commit": HEAD,
            "files": [
                {"path": "projects/alpha/a.md"},
                {"path": "projects/alpha/nested/b.md"},
                {"path": "repo/src/c.py"},
            ],
        }

        async with _baseline_patches(summary=summary) as calls:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert calls.detect.await_args.kwargs["scope_paths"] == {
            "a.md",
            "nested/b.md",
        }


# =============================================================================
# POST /api/jobs/{job_id}/reject
# =============================================================================


@asynccontextmanager
async def _reject_patches(*, loop_id=None):
    with (
        patch(
            "orchestrator.services.project_loops.job_loop_id",
            MagicMock(return_value=loop_id),
        ),
        patch(
            "orchestrator.services.completion.apply_terminal_job_side_effects",
            AsyncMock(return_value={"actions": ["tagged"]}),
        ) as side_effects,
    ):
        yield SimpleNamespace(side_effects=side_effects)


class TestRejectJobDiff:
    @pytest.mark.asyncio
    async def test_the_guard_fires_before_every_gate(self):
        control = _Control(guard_error=HTTPException(409, "completion finalizing"))
        deps, _ = _deps(job=_job(status="completed"), control=control)

        with pytest.raises(HTTPException) as exc:
            await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert control.order == ["guard:mode_a_reject"]

    @pytest.mark.parametrize(
        ("job_over", "detail"),
        [
            (
                {"status": "processing"},
                "Job is in status 'processing'; "
                "only pending_review jobs can be rejected.",
            ),
            ({"project_id": None}, "Job has no project attached; no diff to reject."),
            (
                {"diff_status": "rejected"},
                "Job diff_status is 'rejected'; only pending diffs can be rejected.",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_each_gate_is_a_409_string_and_takes_no_claim(self, job_over, detail):
        deps, control = _deps(job=_job(**job_over))

        with pytest.raises(HTTPException) as exc:
            await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == detail
        assert control.order == ["guard:mode_a_reject"]

    @pytest.mark.asyncio
    async def test_reject_needs_no_baseline_forge_or_cloud_backend(self):
        """Unlike accept: nothing is written to the cloud, so nothing is checked."""

        def unreachable(_project):
            raise AssertionError("reject must not route a cloud backend")

        deps, _ = _deps(
            job=_job(cloud_diff_baseline_commit=None),
            control=_Control(claim=None),
            forge=_forge(False),
            cloud_router=SimpleNamespace(for_project=unreachable),
        )

        async with _reject_patches():
            result = await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert result["diff_status"] == "rejected"

    @pytest.mark.asyncio
    async def test_the_claimed_commit_is_one_guarded_statement(self):
        control = _Control(claim=object(), finish_rows=[{"id": JOB_ID}])
        store = _Store(_project())
        deps, _ = _deps(control=control, store=store)

        async with _reject_patches():
            result = await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert result == {
            "job_id": JOB_ID,
            "diff_status": "rejected",
            "status": "completed",
            "actions": ["tagged"],
        }
        sql, args = control.statements[0]
        assert "diff_status='rejected'" in sql
        assert "merge_status='cloud-rejected'" in sql
        assert args[2] == "pinned"
        assert store.names == []

    @pytest.mark.asyncio
    async def test_a_lost_commit_race_releases_the_claim_and_409s(self):
        claim = object()
        control = _Control(claim=claim, finish_rows=[None])
        deps, _ = _deps(control=control)

        async with _reject_patches():
            with pytest.raises(HTTPException) as exc:
                await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "job changed while Mode A reject was being committed"
        )
        assert control.aborted == [claim]

    @pytest.mark.asyncio
    async def test_the_unclaimed_transition_writes_the_rejection_note(self):
        store = _Store(_project())
        deps, _ = _deps(control=_Control(claim=None), store=store)

        async with _reject_patches():
            await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert store.names == [
            "update_job_cloud_diff",
            "update_job_merge_status",
            "update_job_status",
            "merge_job_context",
        ]
        assert store.calls[3][2]["patch"]["loop_cloud_delivery"] == {
            "delivery_status": "cloud-rejected",
            "needs_review": False,
            "delivery_sha": None,
            "notes": ["project-file diff rejected; cloud folder left unchanged"],
        }

    @pytest.mark.asyncio
    async def test_a_loop_job_advances_the_loop(self):
        advance = AsyncMock()
        deps, _ = _deps(control=_Control(claim=None), advance=advance)

        async with _reject_patches(loop_id="loop-1") as calls:
            await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        advance.assert_awaited_once()
        calls.side_effects.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_execution_lane_default_of_pinned_is_used_when_absent(self):
        control = _Control(claim=object(), finish_rows=[{"id": JOB_ID}])
        deps, _ = _deps(job=_job(execution_lane=None), control=control)

        async with _reject_patches():
            await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert control.statements[0][1][2] == "pinned"


# =============================================================================
# Wire: paths, greedy per-file route order, per-invocation dependency resolution
# =============================================================================


class TestWhoDecidesTheDiff:
    """Seeing a job is not deciding its write-back: a viewer of the project,
    and a job owner since demoted to one, can read the diff but neither
    accept nor reject it; an editor, an owner and an administrator can."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", [accept_job_diff, reject_job_diff])
    @pytest.mark.parametrize(
        "job_over",
        [{}, {"user_id": USER["id"]}],
        ids=["another-members-job", "own-job-after-demotion"],
    )
    async def test_a_viewer_is_refused_before_any_gate_or_claim(
        self, operation, job_over
    ):
        store = _Store(_project(), role="viewer")
        deps, control = _deps(job=_job(**job_over), store=store)

        with pytest.raises(HTTPException) as exc:
            await operation(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 403
        assert "owner or editor of the job's project" in exc.value.detail
        assert store.role_lookups == [(PROJECT_ID, USER["id"])]
        assert store.calls == []
        assert not any(step.startswith("claim:") for step in control.order)

    @pytest.mark.asyncio
    async def test_a_non_member_is_refused(self):
        store = _Store(_project(), role=None)
        deps, _ = _deps(store=store)

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("user", "role"),
        [
            (USER, "editor"),
            (USER, "owner"),
            ({"id": "d6fa6f2a-9101-4c1e-9b9e-0000000000c4", "is_admin": True}, None),
        ],
        ids=["editor", "owner", "administrator-outside-the-project"],
    )
    async def test_an_editor_owner_or_administrator_may_reject(self, user, role):
        store = _Store(_project(), role=role)
        deps, _ = _deps(user=user, store=store)

        async with _reject_patches():
            result = await reject_job_diff(_request(), JOB_ID, dependencies=deps)

        assert result["diff_status"] == "rejected"
        if user.get("is_admin"):
            assert store.role_lookups == []

    @pytest.mark.asyncio
    async def test_a_read_only_copys_diff_is_never_accepted(self):
        """An older orchestrator captured a read-only copy's diff (rollout
        overlap): the accept refuses it and writes nothing; rejecting it is
        the job's way out."""
        job = _job(context={"cloud_checkout": {"access": "read_only"}})
        store = _Store(_project(), role="owner")
        deps, control = _deps(job=job, store=store)

        with pytest.raises(HTTPException) as exc:
            await accept_job_diff(_request(), JOB_ID, dependencies=deps)

        assert exc.value.status_code == 409
        assert "read-only" in exc.value.detail
        assert store.calls == []
        assert not any(step.startswith("claim:") for step in control.order)

        deps, _ = _deps(job=_job(context=job["context"]), store=store)
        async with _reject_patches():
            result = await reject_job_diff(_request(), JOB_ID, dependencies=deps)
        assert result["diff_status"] == "rejected"


class TestJobDiffWire:
    def test_routes_keep_their_paths_and_methods(self):
        app = mount_router(job_diff_router)
        # ``app.routes`` is not the API on FastAPI >= 0.139: an include is one
        # ``_IncludedRouter`` wrapper with no path or methods of its own, so the
        # obvious comprehension silently sees zero of these routes. The pin is
        # ``fastapi>=0.109.0``, so a fresh install resolves the new shape while a
        # long-lived venv sits on the old one — read it through the helper.
        declared = [
            (route.path, tuple(sorted(route.methods)))
            for route in mounted_route_objects(app)
        ]

        assert ("/api/jobs/{job_id}/diff", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/diff/{file_path:path}", ("GET",)) in declared
        assert ("/api/jobs/{job_id}/accept", ("POST",)) in declared
        assert ("/api/jobs/{job_id}/reject", ("POST",)) in declared
        # The summary must stay declared before the greedy per-file path.
        assert declared.index(("/api/jobs/{job_id}/diff", ("GET",))) < declared.index(
            ("/api/jobs/{job_id}/diff/{file_path:path}", ("GET",))
        )

    def test_the_summary_route_still_wins_over_the_greedy_file_route(self):
        deps, _ = _deps()
        factory, _src = _diff_source(summary=_summary())
        app = mount_router(
            job_diff_router,
            factories={"job_diff_dependencies_factory": lambda: deps},
        )

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            with TestClient(app) as client:
                response = client.get(f"/api/jobs/{JOB_ID}/diff")

        assert response.status_code == 200
        assert response.json()["files"] == [
            {"path": "projects/a/x.md", "status": "modified"}
        ]

    def test_a_dict_detail_survives_serialization(self):
        deps, _ = _deps()
        app = mount_router(
            job_diff_router,
            factories={"job_diff_dependencies_factory": lambda: deps},
        )

        async def diverged(**_kw):
            return ["notes.md"]

        with TestClient(app) as client:
            with (
                patch(
                    "orchestrator.services.job_cloud_baseline.get_diff_summary",
                    AsyncMock(return_value={"files": [], "head_commit": HEAD}),
                ),
                patch(
                    "orchestrator.services.job_cloud_baseline.detect_external_mods",
                    diverged,
                ),
                patch(
                    "orchestrator.services.job_cloud_baseline.project_folder_slug",
                    MagicMock(return_value="alpha"),
                ),
            ):
                response = client.post(f"/api/jobs/{JOB_ID}/accept")

        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "external_modifications_detected"

    def test_a_rebound_forge_is_observed_by_the_next_dependency_build(self):
        first, _ = _deps()
        second, _ = _deps(forge=_forge(False))
        holder = {"deps": first}
        factory, _src = _diff_source(summary=_summary())
        app = mount_router(
            job_diff_router,
            factories={"job_diff_dependencies_factory": lambda: holder["deps"]},
        )

        with patch("orchestrator.services.diff_source.GiteaDiffSource", factory):
            with TestClient(app) as client:
                assert client.get(f"/api/jobs/{JOB_ID}/diff").status_code == 200
                holder["deps"] = second
                response = client.get(f"/api/jobs/{JOB_ID}/diff")

        assert response.status_code == 503
        assert response.json() == {"detail": "Gitea not available."}
