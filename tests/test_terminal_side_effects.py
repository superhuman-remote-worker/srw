"""Tests for the §6.6 terminal-transition side effects
(knowledge-base/knowledge/features/workspace_and_change_records.md).

``apply_terminal_job_side_effects`` is the ONE function both ordinary-job
terminal paths call — the ``/complete`` handler's 5d2 hook and ``approve_job``.
New isolated jobs write a structured database record; the curated project-repo
merge below is retained only to characterize in-flight legacy branches:

* a job carrying a FILE contract gets the §6.4 **curated merge** (contracted
  paths only, audit PR closed unmerged) + a change record;
* a job with **no** contract gets the record ONLY and keeps its branch — a
  full squash-merge there would land the whole scratchpad on ``main``, the
  accumulation §6.4 exists to prevent;
* loop jobs, subjobs, failures and already-merged rows never merge here.

Gitea is mocked (pattern: tests/test_loop_merge.py); the two HTTP handlers
are driven for real with patched ``main`` globals (pattern:
tests/test_export_to_cloud_endpoint.py).
"""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

from tests import b08_completion_helpers as b08_helpers

import base64
import dataclasses
import re
import uuid
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main
from orchestrator.routers import job_diff as job_diff_routes
from orchestrator.services.completion import (
    apply_terminal_job_side_effects,
    job_has_file_contract,
    should_merge_job_contribution,
)
from orchestrator.application import workspace as workspace_composition
from orchestrator.schemas import job_runtime as job_runtime_module

JOB_ID = uuid.UUID("abcdef12-3456-7890-abcd-ef1234567890")
COMMAND_ID = "11111111-2222-4333-8444-555555555555"
PROJECT_ID = uuid.UUID("68137e29-1111-2222-3333-444444444444")
REPO = "project-68137e29-jobs"
BRANCH = "job/abcdef12"
CONTRACT = "src/thing.py"
ON_BRANCH = f"repo/{CONTRACT}"  # the checkout spelling the agent writes


# --------------------------------------------------------------------------- #
# Fixtures / helpers
# --------------------------------------------------------------------------- #


def _tree(paths) -> list[dict]:
    return [{"path": p, "type": "blob", "sha": "beef" * 10} for p in paths]


def _make_gitea(
    *,
    branch_files: dict | None = None,
    main_files: list | None = None,
    total_commits: int = 2,
    listing: list | None = None,
) -> MagicMock:
    """Mocked gitea surface covering the merge, the record and approve's own
    ``job_completion.json`` bookkeeping."""
    branch_files = (
        {ON_BRANCH: b"print('recovered')\n"} if branch_files is None else branch_files
    )
    main_files = main_files or ["README.md"]

    g = MagicMock()
    g.is_initialized = True
    g.get_compare = AsyncMock(return_value={"total_commits": total_commits})
    g.create_pr = AsyncMock(return_value={"number": 7, "url": "http://g/pr/7"})
    g.merge_pr = AsyncMock(return_value=True)
    g.probe_pr_merged = AsyncMock(return_value=None)
    g.list_pull_requests = AsyncMock(side_effect=lambda *args, **kw: [])
    g.get_commits = AsyncMock(side_effect=lambda *args, **kw: [])
    g.close_pr = AsyncMock(return_value=True)
    g.comment_on_pr = AsyncMock(return_value=True)
    g.get_branch_head_sha = AsyncMock(return_value="c0ffee00" * 5)
    g.change_files = AsyncMock(return_value=True)
    g.list_contents = AsyncMock(return_value=listing if listing is not None else [])
    g.get_file = AsyncMock(return_value=None)
    g.create_or_update_file = AsyncMock(return_value=True)
    g.delete_file = AsyncMock(return_value=True)

    async def _list_tree(repo: str, ref: str):
        return _tree(main_files) if ref == "main" else _tree(list(branch_files))

    g.list_tree = AsyncMock(side_effect=_list_tree)
    g.get_file_bytes = AsyncMock(
        side_effect=lambda repo, path, ref=None: branch_files.get(path)
    )
    return g


class _FakeDB:
    """postgres_db stand-in: persists merge_status back onto the job row so a
    LATER call can see it (that is what makes the double-merge backstop real
    across requests)."""

    def __init__(self, job: dict, *, project: dict | None = None) -> None:
        self.job = job
        self.project = project
        self.merge_status_writes: list[str] = []
        self.status_writes: list[dict] = []
        self.executed: list[str] = []
        self.records: list[dict] = []
        self.context_writes: list[dict] = []

    async def get_job(self, job_id: str) -> dict:
        return self.job

    async def get_project(self, project_id: str) -> dict | None:
        return self.project

    async def update_job_merge_status(self, job_id, merge_status=None, **kw) -> bool:
        self.merge_status_writes.append(merge_status)
        self.job["merge_status"] = merge_status
        return True

    async def update_job_status(self, job_id, **kw) -> bool:
        self.status_writes.append(kw)
        self.job.update(kw)
        return True

    async def update_job_cloud_diff(self, job_id, **kw) -> bool:
        self.job.update(kw)
        return True

    async def merge_job_context(self, job_id, payload) -> bool:
        self.context_writes.append(payload)
        context = self.job.get("context") or {}
        if not isinstance(context, dict):
            context = {}
        context.update(payload)
        self.job["context"] = context
        return True

    async def create_job_change_record(self, **kwargs) -> bool:
        if any(row["job_id"] == kwargs["job_id"] for row in self.records):
            return False
        self.records.append(kwargs)
        return True

    def acquire(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(side_effect=lambda sql, *a: self.executed.append(sql))
        cm = AsyncMock()
        cm.__aenter__.return_value = conn
        cm.__aexit__.return_value = False
        return cm


def _job(**over) -> dict:
    row = {
        "id": JOB_ID,
        "status": "pending_review",
        "project_id": PROJECT_ID,
        "parent_job_id": None,
        "repo_name": REPO,
        "branch_name": BRANCH,
        "merge_status": None,
        "config_name": "developer",
        "description": "Implement line recovery",
        "freeze_data": {"freeze_type": "job_complete", "notes": "Shipped AC-11."},
        "context": {"required_deliverables": [CONTRACT]},
        "cloud_diff_baseline_commit": None,
        "project_has_cloud_folder": False,
        "resolved_config": {},
        "user_id": None,
        "diff_status": None,
    }
    row.update(over)
    return row


_TS = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00")
_STAMP = re.compile(r"\d{8}-\d{6}")


def _norm(text: str) -> str:
    """Blank out the two wall-clock stamps so two runs of the same work are
    byte-comparable (the record's ``created:`` and its filename prefix)."""
    return _STAMP.sub("<stamp>", _TS.sub("<ts>", text))


def _commits(g: MagicMock) -> list[dict]:
    """Every commit gitea was asked to make, normalized — the observable
    effect of this transition on ``main``."""
    out = []
    for call in g.change_files.await_args_list:
        repo, branch, files = call.args[0], call.args[1], call.args[2]
        out.append(
            {
                "repo": repo,
                "branch": branch,
                "message": _norm(call.kwargs.get("message", "")),
                "files": [(_norm(f["path"]), f.get("operation")) for f in files],
                "content": [
                    _norm(base64.b64decode(f["content_b64"]).decode()) for f in files
                ],
            }
        )
    return out


def _ceremony(g: MagicMock) -> dict[str, list[str]]:
    """The PR ceremony trace (create/merge/close/comment), normalized."""
    return {
        name: [_norm(str(c)) for c in getattr(g, name).await_args_list]
        for name in ("create_pr", "merge_pr", "close_pr", "comment_on_pr")
    }


def _record_text(g: MagicMock) -> str:
    """The change record's rendered body (the last commit into ``retros/``)."""
    for call in reversed(g.change_files.await_args_list):
        entry = call.args[2][0]
        if entry["path"].startswith("retros/"):
            return base64.b64decode(entry["content_b64"]).decode()
    raise AssertionError("no record commit found")


# --------------------------------------------------------------------------- #
# The contract predicate + the gate
# --------------------------------------------------------------------------- #


class TestMergeGate:
    def test_file_contract_detected_from_required_deliverables(self) -> None:
        assert job_has_file_contract(_job()) is True
        assert job_has_file_contract(_job(context={})) is False
        assert job_has_file_contract(_job(context=None)) is False
        # kb: entries are store-backed — never a FILE contract
        assert (
            job_has_file_contract(
                _job(context={"required_deliverables": ["kb:some-note"]})
            )
            is False
        )

    def test_predicate_is_the_one_the_merge_dispatches_on(self) -> None:
        """Our gate and ``merge_loop_job_contribution``'s curated-vs-full
        dispatch MUST ask the same question: a disagreement means calling in
        for a job the merge considers contract-less, i.e. a FULL squash-merge
        of the whole scratchpad onto ``main``."""
        from orchestrator.services.project_loops import contracted_file_deliverables

        for context in (
            {"required_deliverables": [CONTRACT]},
            {"required_deliverables": ["repo/src/thing.py", "./out.md"]},
            {"required_deliverables": ["kb:note", CONTRACT]},
            {"required_deliverables": ["kb:note"]},
            {"required_deliverables": []},
            {"required_deliverables": "single.md"},
            {"required_deliverables": [None, "", 7]},
            {},
            None,
        ):
            job = _job(context=context)
            assert job_has_file_contract(job) is bool(
                contracted_file_deliverables(job)
            ), context

    @pytest.mark.parametrize(
        "job,new_status,expect",
        [
            (_job(), "completed", "file contract"),
            (_job(), "failed", "not a successful completion"),
            (_job(context={}), "completed", "no file contract"),
            (_job(parent_job_id=uuid.uuid4()), "completed", "subjob"),
            (
                _job(
                    context={"loop_id": "1a387b4d", "required_deliverables": [CONTRACT]}
                ),
                "completed",
                "loop job",
            ),
            (_job(merge_status="merged"), "completed", "already merged"),
            (_job(merge_status="curated"), "completed", "already curated"),
            (_job(project_id=None), "completed", "no project"),
            (_job(branch_name=None), "completed", "no project branch"),
            (_job(branch_name="main"), "completed", "no project branch"),
            (_job(repo_name=None), "completed", "no project branch"),
            (
                _job(repo_name="job-abcdef12", branch_name=None),
                "completed",
                "no project branch",
            ),
        ],
    )
    def test_gate_reasons(self, job: dict, new_status: str, expect: str) -> None:
        should, reason = should_merge_job_contribution(job, new_status)
        assert should is (expect == "file contract")
        assert expect in reason

    def test_per_job_repo_is_a_no_op_even_with_a_branch(self) -> None:
        """A project with no jobs repo falls back to ``job-<short_id>``; the
        agent works on that repo's own ``main``, so there is nothing to merge
        into."""
        should, reason = should_merge_job_contribution(
            _job(repo_name="job-abcdef12", branch_name="job/abcdef12"), "completed"
        )
        assert should is False
        assert "per-job repo" in reason


# --------------------------------------------------------------------------- #
# The shared function
# --------------------------------------------------------------------------- #


class TestApplyTerminalJobSideEffects:
    @pytest.mark.asyncio
    async def test_file_contract_gets_curated_merge_plus_record(self) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] == "curated"
        assert out["merged_sha"] == "c0ffee00" * 5
        assert out["record_written"] is True
        assert out["actions"] == [
            "branch merge -> curated",
            "job change record written to database",
        ]

        commits = _commits(g)
        assert len(commits) == 1
        curated = commits[0]
        # ONE commit onto main carrying ONLY the contracted deliverable.
        assert curated["branch"] == "main"
        assert curated["files"] == [(ON_BRANCH, "create")]
        assert curated["message"].startswith("curated merge: developer (abcdef12)")
        record = db.records[0]
        assert record["delivery_status"] == "curated"
        assert record["delivery_sha"] == "c0ffee00" * 5
        assert record["branch_name"] == BRANCH
        assert record["changes"][0]["action"] == "merge"

        # Never the full squash-merge; the audit PR closes UNMERGED.
        g.merge_pr.assert_not_called()
        g.close_pr.assert_awaited_once()
        # The outcome is persisted, which is what makes a second call a no-op.
        assert db.merge_status_writes == ["curated"]
        assert job["merge_status"] == "curated"

    @pytest.mark.asyncio
    async def test_no_contract_writes_the_record_and_leaves_the_branch(self) -> None:
        """§6.6 policy: no contract → NO merge at all. A full squash-merge
        here would land plan.md/todos.yaml/archive/ on main."""
        job = _job(context={})
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] is None
        assert "no file contract" in out["merge_skipped_reason"]
        assert out["record_written"] is True
        assert out["actions"] == ["job change record written to database"]

        # Branch untouched: nothing compared, no PR, no merge, no curation.
        g.get_compare.assert_not_called()
        g.create_pr.assert_not_called()
        g.merge_pr.assert_not_called()
        g.list_tree.assert_not_called()
        assert db.merge_status_writes == []

        assert _commits(g) == []
        assert db.records[0]["branch_name"] == BRANCH
        assert db.records[0]["delivery_status"] == "isolated"

    @pytest.mark.asyncio
    async def test_kb_only_contract_is_also_no_merge(self) -> None:
        job = _job(context={"required_deliverables": ["kb:findings"]})
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] is None
        g.get_compare.assert_not_called()
        assert out["record_written"] is True

    @pytest.mark.asyncio
    async def test_failed_job_records_but_does_not_merge(self) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(
            job, "failed", gitea=g, db=db, error="boom"
        )

        assert out["merge_status"] is None
        assert "not a successful completion" in out["merge_skipped_reason"]
        g.get_compare.assert_not_called()
        assert out["record_written"] is True
        assert db.records[0]["error"] == "boom"

    @pytest.mark.asyncio
    async def test_blocked_delivery_records_truthful_outcome_without_merge(
        self,
    ) -> None:
        job = _job(context={"required_deliverables": ["pr:acme/widget"]})
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(
            job,
            "cancelled",
            gitea=g,
            db=db,
            error="pull request could not be delivered",
            outcome_kind="blocked_undelivered",
        )

        assert out["record_written"] is True
        assert out["merge_status"] is None
        g.get_compare.assert_not_called()
        assert db.records[0]["status"] == "blocked_undelivered"
        assert db.records[0]["error"] == "pull request could not be delivered"

    @pytest.mark.asyncio
    async def test_non_terminal_status_does_nothing(self) -> None:
        g = _make_gitea()
        out = await apply_terminal_job_side_effects(_job(), "pending_review", gitea=g)
        assert out == {
            "merge_status": None,
            "merged_sha": None,
            "merge_notes": [],
            "merge_skipped_reason": None,
            "record_written": False,
            "actions": [],
        }
        g.change_files.assert_not_called()

    @pytest.mark.asyncio
    async def test_subjob_is_left_to_the_delegation_graft(self) -> None:
        job = _job(parent_job_id=uuid.uuid4(), branch_name="subjob/abcdef12/critic")
        g = _make_gitea()
        db = _FakeDB(job)

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert "subjob" in out["merge_skipped_reason"]
        g.get_compare.assert_not_called()
        g.merge_pr.assert_not_called()


class TestNoDoubleMerge:
    @pytest.mark.asyncio
    async def test_loop_job_called_twice_never_merges_or_records(self) -> None:
        """Loop jobs merge during the loop advance and their retro IS their
        record — both side effects must stay out of this path however often
        it fires."""
        job = _job(context={"loop_id": "1a387b4d", "required_deliverables": [CONTRACT]})
        g = _make_gitea()
        db = _FakeDB(job)

        first = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)
        second = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        for out in (first, second):
            assert out["merge_status"] is None
            assert "loop job" in out["merge_skipped_reason"]
            assert out["record_written"] is False
        g.get_compare.assert_not_called()
        g.change_files.assert_not_called()  # no merge, no record
        assert db.merge_status_writes == []

    @pytest.mark.asyncio
    async def test_loop_job_that_already_merged_is_caught_by_the_row_backstop(
        self,
    ) -> None:
        """Belt to the loop-stamp's braces: even if the stamp were missing,
        the ``merged``/``curated`` row written by the loop advance blocks a
        second merge."""
        for landed in ("merged", "curated"):
            job = _job(merge_status=landed)
            g = _make_gitea()
            out = await apply_terminal_job_side_effects(job, "completed", gitea=g)
            assert out["merge_status"] is None
            assert f"already {landed}" in out["merge_skipped_reason"]
            g.get_compare.assert_not_called()

    @pytest.mark.asyncio
    async def test_second_terminal_call_on_one_job_merges_once(self) -> None:
        """approve → a late /complete report for the same job: the persisted
        merge_status makes the second call a no-op."""
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)

        first = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)
        second = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert first["merge_status"] == "curated"
        assert second["merge_status"] is None
        assert "already curated" in second["merge_skipped_reason"]
        assert db.merge_status_writes == ["curated"]
        # One curated commit total (the record writer's own exists-check is
        # exercised separately in tests/test_job_records.py).
        curated = [c for c in _commits(g) if c["message"].startswith("curated merge")]
        assert len(curated) == 1


class TestBestEffort:
    @pytest.mark.asyncio
    async def test_durable_callbacks_reconcile_full_merge_fallback(self) -> None:
        job = _job()
        # No contracted file exists, so curation deliberately falls back to
        # the full PR merge -- the S33 path whose exact PR must survive a kill.
        g = _make_gitea(branch_files={})
        db = _FakeDB(job)
        intent = None

        async def load():
            return intent

        async def store(detail):
            nonlocal intent
            intent = detail
            return detail

        g.merge_pr.side_effect = RuntimeError("killed after merge response")
        with pytest.raises(RuntimeError, match="killed after merge"):
            await apply_terminal_job_side_effects(
                job,
                "completed",
                gitea=g,
                db=db,
                load_merge_intent=load,
                store_merge_intent=store,
                completion_command_id=COMMAND_ID,
            )

        g.probe_pr_merged.return_value = True
        g.merge_pr.side_effect = None
        out = await apply_terminal_job_side_effects(
            job,
            "completed",
            gitea=g,
            db=db,
            load_merge_intent=load,
            store_merge_intent=store,
            completion_command_id=COMMAND_ID,
        )

        assert out["merge_status"] == "merged"
        assert out["record_written"] is True
        g.probe_pr_merged.assert_awaited_once_with(REPO, 7)
        # Exactly the first, possibly-successful POST; replay only probes.
        assert g.merge_pr.await_count == 1
        assert g.create_pr.await_count == 1

    @pytest.mark.asyncio
    async def test_merge_failure_does_not_block_the_record(self) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)
        g.get_compare = AsyncMock(side_effect=RuntimeError("gitea down"))

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] == "merge-failed"
        assert out["record_written"] is True
        assert db.records[0]["delivery_status"] == "merge-failed"
        # Recorded, and NOT in the already-merged vocabulary — a re-run may
        # still merge (legacy compatibility remains retryable).
        assert db.merge_status_writes == ["merge-failed"]
        should, _ = should_merge_job_contribution(job, "completed")
        assert should is True

    @pytest.mark.asyncio
    async def test_record_failure_does_not_raise(self) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)
        db.create_job_change_record = AsyncMock(side_effect=RuntimeError("db down"))

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] == "curated"
        assert out["record_written"] is False

    @pytest.mark.asyncio
    async def test_merge_status_persistence_failure_is_swallowed(self) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)
        db.update_job_merge_status = AsyncMock(side_effect=RuntimeError("db down"))

        out = await apply_terminal_job_side_effects(job, "completed", gitea=g, db=db)

        assert out["merge_status"] == "curated"
        assert out["record_written"] is True
        # The in-memory row still knows, so a same-request re-entry is a no-op.
        assert job["merge_status"] == "curated"


# --------------------------------------------------------------------------- #
# The two call sites, driven for real
# --------------------------------------------------------------------------- #


def _patch_approve(stack: ExitStack, job: dict, gitea: MagicMock, db: _FakeDB, tmp):
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_internal_or_job_access",
            AsyncMock(return_value=(None, job)),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.gitea_client", gitea)
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.vector_db", None))
    stack.enter_context(
        patch(
            "orchestrator.services.subjob_output.resolve_job_repo",
            AsyncMock(return_value=(REPO, BRANCH)),
        )
    )
    stack.enter_context(
        patch("orchestrator.services.session_wake.maybe_wake_session", AsyncMock())
    )
    stack.enter_context(
        patch("orchestrator.services.session_wake.kick_drain", MagicMock())
    )
    stack.enter_context(
        patch("orchestrator.services.job_dispatcher.trigger_dispatch", MagicMock())
    )
    ws = MagicMock()
    ws.base_path = tmp / "workspace"
    stack.enter_context(patch("orchestrator.services.workspace.workspace_service", ws))


def _patch_complete(stack: ExitStack, job: dict, gitea: MagicMock, db: _FakeDB):
    stack.enter_context(
        patch("orchestrator.security.access.require_internal", AsyncMock())
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.gitea_client", gitea)
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.vector_db", None))
    stack.enter_context(
        patch(
            "orchestrator.services.completion.apply_deliverable_gate",
            AsyncMock(side_effect=lambda j, r, s, **kw: (s, [], False)),
        )
    )
    for target in (
        "orchestrator.services.verification_workflow.handle_critic_verdict_on_complete",
        "orchestrator.services.subjob_completion.handle_scholar_completion",
        "orchestrator.services.subjob_completion.handle_delegation_child_completion",
        "orchestrator.services.project_loop_advance.advance_project_loop",
    ):
        stack.enter_context(patch(target, AsyncMock(return_value=[])))
    stack.enter_context(
        patch.object(
            control_seams,
            "archive_and_cleanup_workspace",
            AsyncMock(return_value=[]),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.verification_workflow.trigger_verification_on_complete",
            AsyncMock(return_value=None),
        )
    )
    stack.enter_context(
        patch("orchestrator.services.session_wake.maybe_wake_session", AsyncMock())
    )
    stack.enter_context(
        patch("orchestrator.services.session_wake.kick_drain", MagicMock())
    )
    stack.enter_context(
        patch("orchestrator.services.job_dispatcher.trigger_dispatch", MagicMock())
    )


class TestApproveJobCallSite:
    @pytest.mark.asyncio
    async def test_approval_merges_and_records(self, tmp_path) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)

        with ExitStack() as stack:
            _patch_approve(stack, job, g, db, tmp_path)
            result = await control_seams.approve_job(MagicMock(), str(JOB_ID), None)

        assert result["status"] == "approved"
        messages = [c["message"] for c in _commits(g)]
        assert any(m.startswith("curated merge:") for m in messages)
        assert len(db.records) == 1
        assert db.records[0]["record_type"] == "job_record"
        assert db.merge_status_writes == ["curated"]

    @pytest.mark.asyncio
    async def test_side_effect_failure_never_fails_the_approval(self, tmp_path) -> None:
        job = _job()
        g = _make_gitea()
        db = _FakeDB(job)

        with ExitStack() as stack:
            _patch_approve(stack, job, g, db, tmp_path)
            stack.enter_context(
                patch(
                    "orchestrator.services.completion.apply_terminal_job_side_effects",
                    AsyncMock(side_effect=RuntimeError("gitea down")),
                )
            )
            result = await control_seams.approve_job(MagicMock(), str(JOB_ID), None)

        assert result["status"] == "approved"

    @pytest.mark.asyncio
    async def test_approving_a_no_contract_job_leaves_its_branch(
        self, tmp_path
    ) -> None:
        job = _job(context={})
        g = _make_gitea()
        db = _FakeDB(job)

        with ExitStack() as stack:
            _patch_approve(stack, job, g, db, tmp_path)
            await control_seams.approve_job(MagicMock(), str(JOB_ID), None)

        g.merge_pr.assert_not_called()
        g.create_pr.assert_not_called()
        assert _commits(g) == []
        assert len(db.records) == 1


class TestRejectStaysMergeFree:
    @pytest.mark.asyncio
    async def test_reject_never_merges(self) -> None:
        """``/reject`` is approve's counterpart: the branch stays as it is."""
        job = _job(diff_status="pending")
        g = _make_gitea()
        db = _FakeDB(job)

        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", db)
            )
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.gitea_client", g)
            )
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.vector_db", None)
            )
            result = await job_diff_routes.reject_job_diff(
                MagicMock(),
                str(JOB_ID),
                dependencies=dataclasses.replace(
                    workspace_composition.job_diff_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                    require_job_access=AsyncMock(return_value=({}, job)),
                ),
            )

        assert result["diff_status"] == "rejected"
        g.get_compare.assert_not_called()
        g.create_pr.assert_not_called()
        g.merge_pr.assert_not_called()
        g.change_files.assert_not_called()
        assert db.merge_status_writes == ["cloud-rejected"]
        assert db.records[0]["delivery_status"] == "cloud-rejected"


class TestLoopCloudCompletion:
    @pytest.mark.asyncio
    async def test_late_outage_callback_cannot_reopen_completed_loop_job(
        self,
    ) -> None:
        completion_freeze = {
            "status": "job_completed",
            "notes": "Cloud delivery already completed.",
        }
        job = _job(
            status="completed",
            repo_name="job-abcdef12",
            branch_name=None,
            merge_status="cloud-applied",
            diff_status="accepted",
            freeze_data=completion_freeze,
            context={
                "loop_id": "105a6f98-134c-4077-b7e1-6d08916650d7",
                "loop_role": "developer",
                "loop_cloud_delivery": {
                    "delivery_status": "cloud-applied",
                    "needs_review": False,
                },
            },
        )
        db = _FakeDB(job)
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "llm_unavailable",
                "next_retry_at": "2026-08-04T19:30:00+00:00",
            },
        )

        with ExitStack() as stack:
            _patch_complete(stack, job, _make_gitea(), db)
            handled = await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert handled == {
            "status": "handled",
            "job_id": str(JOB_ID),
            "new_status": "completed",
            "actions": ["late callback ignored; job already completed"],
        }
        assert job["status"] == "completed"
        assert job["freeze_data"] == completion_freeze
        assert db.status_writes == []
        assert db.executed == []

    @pytest.mark.asyncio
    async def test_late_outage_callback_cannot_bypass_loop_cloud_review(
        self,
    ) -> None:
        completion_freeze = {
            "status": "job_completed",
            "notes": "The isolated diff is waiting for conflict review.",
        }
        job = _job(
            status="pending_review",
            repo_name="job-abcdef12",
            branch_name=None,
            merge_status="cloud-conflict",
            diff_status="pending",
            freeze_data=completion_freeze,
            context={
                "loop_id": "105a6f98-134c-4077-b7e1-6d08916650d7",
                "loop_role": "developer",
                "loop_cloud_delivery": {
                    "delivery_status": "cloud-conflict",
                    "needs_review": True,
                },
            },
        )
        db = _FakeDB(job)
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={
                "freeze_type": "llm_unavailable",
                "next_retry_at": "2026-08-04T19:30:00+00:00",
            },
        )

        with ExitStack() as stack:
            _patch_complete(stack, job, _make_gitea(), db)
            handled = await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert handled["new_status"] == "pending_review"
        assert handled["actions"] == [
            "late callback ignored; job already pending_review"
        ]
        assert job["status"] == "pending_review"
        assert job["freeze_data"] == completion_freeze
        assert db.status_writes == []
        assert db.executed == []

    @pytest.mark.asyncio
    async def test_clean_loop_delivery_completes_and_advances(self) -> None:
        job = _job(
            status="processing",
            repo_name="job-abcdef12",
            branch_name=None,
            cloud_diff_baseline_commit="a" * 40,
            context={
                "loop_id": "105a6f98-134c-4077-b7e1-6d08916650d7",
                "loop_role": "developer",
                "cloud_baseline": {"state": "ready", "entries": {}},
            },
        )
        project = {
            "id": PROJECT_ID,
            "name": "Cloud Project",
            "main_cloud_backend": "opencloud",
            "main_cloud_folder_handle": "opaque",
        }
        db = _FakeDB(job, project=project)
        gitea = _make_gitea()
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data=job["freeze_data"],
        )
        delivery_result = {
            "delivery_status": "cloud-applied",
            "needs_review": False,
            "delivery_sha": "b" * 40,
            "notes": [],
            "applied": 1,
            "deleted": 0,
        }

        with ExitStack() as stack:
            _patch_complete(stack, job, gitea, db)
            advance = stack.enter_context(
                patch(
                    "orchestrator.services.project_loop_advance.advance_project_loop",
                    new_callable=AsyncMock,
                )
            )
            deliver = stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.deliver_loop_diff_to_cloud",
                    AsyncMock(return_value=delivery_result),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert handled["new_status"] == "completed"
        assert job["merge_status"] == "cloud-applied"
        assert job["context"]["loop_cloud_delivery"] == delivery_result
        deliver.assert_awaited_once()
        advance.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_loop_cloud_conflict_parks_without_advancing(self) -> None:
        job = _job(
            status="processing",
            repo_name="job-abcdef12",
            branch_name=None,
            cloud_diff_baseline_commit="a" * 40,
            context={
                "loop_id": "105a6f98-134c-4077-b7e1-6d08916650d7",
                "loop_role": "developer",
                "cloud_baseline": {"state": "ready", "entries": {}},
            },
        )
        project = {
            "id": PROJECT_ID,
            "name": "Cloud Project",
            "main_cloud_backend": "opencloud",
            "main_cloud_folder_handle": "opaque",
        }
        db = _FakeDB(job, project=project)
        gitea = _make_gitea()
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data=job["freeze_data"],
        )

        with ExitStack() as stack:
            _patch_complete(stack, job, gitea, db)
            advance = stack.enter_context(
                patch(
                    "orchestrator.services.project_loop_advance.advance_project_loop",
                    new_callable=AsyncMock,
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.deliver_loop_diff_to_cloud",
                    AsyncMock(
                        return_value={
                            "delivery_status": "cloud-conflict",
                            "needs_review": True,
                            "delivery_sha": "b" * 40,
                            "notes": ["cloud changed since baseline: report.md"],
                        }
                    ),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert handled["new_status"] == "pending_review"
        assert job["status"] == "pending_review"
        assert job["merge_status"] == "cloud-conflict"
        advance.assert_not_awaited()


class TestBothPathsAgree:
    @pytest.mark.asyncio
    async def test_approve_and_complete_produce_the_same_effects(
        self, tmp_path
    ) -> None:
        """The point of §6.6: ONE code path. For the SAME job, approval (the
        ``review``-autonomy transition) and a self-sealing ``/complete``
        (``full`` autonomy) must leave byte-identical commits on ``main`` and
        run the identical PR ceremony — not merely "both ran"."""
        approve_job_row = _job()
        g_approve = _make_gitea()
        db_approve = _FakeDB(approve_job_row)
        with ExitStack() as stack:
            _patch_approve(stack, approve_job_row, g_approve, db_approve, tmp_path)
            await control_seams.approve_job(MagicMock(), str(JOB_ID), None)

        complete_job_row = _job(status="processing")
        g_complete = _make_gitea()
        db_complete = _FakeDB(complete_job_row)
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data=complete_job_row["freeze_data"],
        )
        with ExitStack() as stack:
            _patch_complete(stack, complete_job_row, g_complete, db_complete)
            handled = await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert handled["new_status"] == "completed"
        assert "job change record written to database" in handled["actions"]
        assert "branch merge -> curated" in handled["actions"]

        assert len(_commits(g_approve)) == 1  # curated legacy compatibility merge
        assert _commits(g_approve) == _commits(g_complete)
        assert _ceremony(g_approve) == _ceremony(g_complete)
        assert db_approve.records == db_complete.records
        assert approve_job_row["merge_status"] == complete_job_row["merge_status"]
        assert approve_job_row["merge_status"] == "curated"

    @pytest.mark.asyncio
    async def test_both_paths_agree_for_a_job_with_no_contract(self, tmp_path) -> None:
        approve_job_row = _job(context={})
        g_approve = _make_gitea()
        db_approve = _FakeDB(approve_job_row)
        with ExitStack() as stack:
            _patch_approve(stack, approve_job_row, g_approve, db_approve, tmp_path)
            await control_seams.approve_job(MagicMock(), str(JOB_ID), None)

        complete_job_row = _job(status="processing", context={})
        g_complete = _make_gitea()
        db_complete = _FakeDB(complete_job_row)
        body = job_runtime_module.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data=complete_job_row["freeze_data"],
        )
        with ExitStack() as stack:
            _patch_complete(stack, complete_job_row, g_complete, db_complete)
            await b08_helpers.complete_job(MagicMock(), str(JOB_ID), body)

        assert _commits(g_approve) == _commits(g_complete)
        assert len(_commits(g_approve)) == 0
        assert db_approve.records == db_complete.records
        assert _ceremony(g_approve) == _ceremony(g_complete)
