"""Merged-PR completion grant — the capability read.

Spec: knowledge-base/knowledge/features/merged_pr_completion_grant.md. Mirrors
tests/test_public_datasources.py, whose helper this one is modelled on.
"""

from tests import _b09_control_seams as control_seams

from unittest.mock import AsyncMock

import pytest

from orchestrator.database.postgres import PostgresDB

pytestmark = pytest.mark.asyncio

EMPTY_SCOPES = {"user": [], "project": [], "global": []}
PROJECT_ID = "22222222-2222-2222-2222-222222222222"


def _db_with_grant_rows(scoped):
    """PostgresDB with no pool — only list_grants_for_scopes is exercised."""
    db = PostgresDB.__new__(PostgresDB)
    db.list_grants_for_scopes = AsyncMock(return_value=scoped)
    return db


class TestUserCanCompleteUnmergedPr:
    async def test_admin_short_circuits_without_grant_read(self):
        db = _db_with_grant_rows(EMPTY_SCOPES)
        assert (
            await db.user_can_complete_unmerged_pr(
                {"id": "u1", "is_admin": True}, PROJECT_ID
            )
            is True
        )
        db.list_grants_for_scopes.assert_not_awaited()

    async def test_no_rows_denies_by_default(self):
        db = _db_with_grant_rows(EMPTY_SCOPES)
        assert (
            await db.user_can_complete_unmerged_pr(
                {"id": "u1", "is_admin": False}, PROJECT_ID
            )
            is False
        )

    async def test_user_scope_grant_allows(self):
        db = _db_with_grant_rows(
            {
                "user": [{"key": "complete_unmerged_pr", "value_json": True}],
                "project": [],
                "global": [],
            }
        )
        assert (
            await db.user_can_complete_unmerged_pr(
                {"id": "u1", "is_admin": False}, PROJECT_ID
            )
            is True
        )

    async def test_project_scope_grant_allows(self):
        """The axis a projects column could not express, and the reason this is
        a grant at all: policy that varies per project, not per user."""
        db = _db_with_grant_rows(
            {
                "user": [],
                "project": [{"key": "complete_unmerged_pr", "value_json": True}],
                "global": [],
            }
        )
        assert (
            await db.user_can_complete_unmerged_pr(
                {"id": "u1", "is_admin": False}, PROJECT_ID
            )
            is True
        )

    async def test_the_jobs_project_is_actually_queried(self):
        """A project-scope grant can only resolve if the job's project id
        reaches list_grants_for_scopes — passing [] would silently reduce this
        to a user-only capability."""
        db = _db_with_grant_rows(EMPTY_SCOPES)
        await db.user_can_complete_unmerged_pr(
            {"id": "u1", "is_admin": False}, PROJECT_ID
        )
        kwargs = db.list_grants_for_scopes.await_args.kwargs
        assert kwargs["project_ids"] == [PROJECT_ID]

    async def test_no_project_still_reads_user_and_global_scopes(self):
        db = _db_with_grant_rows(EMPTY_SCOPES)
        await db.user_can_complete_unmerged_pr({"id": "u1", "is_admin": False}, None)
        assert db.list_grants_for_scopes.await_args.kwargs["project_ids"] == []

    async def test_grant_read_failure_fails_closed(self):
        db = PostgresDB.__new__(PostgresDB)
        db.list_grants_for_scopes = AsyncMock(side_effect=RuntimeError("db down"))
        assert (
            await db.user_can_complete_unmerged_pr(
                {"id": "u1", "is_admin": False}, PROJECT_ID
            )
            is False
        )


# --- The live pull-request predicate ----------------------------------------

REPO = "Knaeckebrothero/KurortEngine"
PR_URL = f"https://github.com/{REPO}/pull/1"


def _job(pull_request=True, **overrides):
    context = {}
    if pull_request:
        context["pull_request"] = {
            "forge": "github",
            "repo": REPO,
            "number": 1,
            "url": PR_URL,
            "head": "design/hotel-rheinland-theme",
            "base": "main",
        }
    return {"id": "job-1", "context": context, **overrides}


def _repo_datasource():
    return {
        "id": "ds-1",
        "type": "repository",
        "config": {"forge": "github"},
        "connection_url": f"https://github.com/{REPO}.git",
        "credentials": {"token": "t"},
    }


def _status(state):
    return {
        "number": 1,
        "url": PR_URL,
        "state": state,
        "head": "design/hotel-rheinland-theme",
        "base": "main",
        "draft": False,
    }


class TestUnmergedPrBlockReason:
    async def test_configured_gitea_public_url_matches_internal_connector(
        self, monkeypatch
    ):
        from orchestrator.services import job_delivery

        monkeypatch.setenv("GITEA_INTERNAL_URL", "http://srw-gitea:3000")
        monkeypatch.setenv("GITEA_URL", "https://git.srw.works")
        datasource = {
            "id": "ds-1",
            "type": "repository",
            "config": {"forge": "gitea"},
            "connection_url": "http://srw-gitea:3000/acme/widget.git",
            "credentials": {"token": "t"},
        }
        job = _job()
        job["context"]["pull_request"].update(
            {
                "forge": "gitea",
                "repo": "acme/widget",
                "url": "https://git.srw.works/acme/widget/pulls/1",
            }
        )
        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(return_value=_status("merged")),
        )

        assert (
            await job_delivery.unmerged_pr_block_reason(job, datasources=[datasource])
            is None
        )

    async def test_global_gitea_public_url_cannot_alias_a_foreign_connector(
        self, monkeypatch
    ):
        from orchestrator.services import job_delivery

        monkeypatch.setenv("GITEA_INTERNAL_URL", "http://srw-gitea:3000")
        monkeypatch.setenv("GITEA_URL", "https://git.srw.works")
        datasource = {
            "id": "ds-1",
            "type": "repository",
            "config": {"forge": "gitea"},
            "connection_url": "https://foreign.example/acme/widget.git",
            "credentials": {"token": "t"},
        }
        job = _job()
        job["context"]["pull_request"].update(
            {
                "forge": "gitea",
                "repo": "acme/widget",
                "url": "https://git.srw.works/acme/widget/pulls/1",
            }
        )
        status = AsyncMock(return_value=_status("merged"))
        monkeypatch.setattr(job_delivery, "get_pull_request_status", status)

        reason = await job_delivery.unmerged_pr_block_reason(
            job, datasources=[datasource]
        )
        assert reason is not None
        status.assert_not_awaited()

    async def test_job_without_a_pull_request_is_never_blocked(self, monkeypatch):
        """The accepted hole, pinned so it stays deliberate: a job that never
        opened a PR must approve exactly as it does today."""
        from orchestrator.services import job_delivery

        called = []
        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(side_effect=lambda *a, **k: called.append(1)),
        )
        assert (
            await job_delivery.unmerged_pr_block_reason(
                _job(pull_request=False), datasources=[_repo_datasource()]
            )
            is None
        )
        assert not called

    async def test_merged_pull_request_does_not_block(self, monkeypatch):
        from orchestrator.services import job_delivery

        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(return_value=_status("merged")),
        )
        assert (
            await job_delivery.unmerged_pr_block_reason(
                _job(), datasources=[_repo_datasource()]
            )
            is None
        )

    async def test_open_pull_request_blocks_and_names_the_state(self, monkeypatch):
        from orchestrator.services import job_delivery

        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(return_value=_status("open")),
        )
        reason = await job_delivery.unmerged_pr_block_reason(
            _job(), datasources=[_repo_datasource()]
        )
        assert reason is not None
        assert "open" in reason.lower()
        assert "#1" in reason

    async def test_closed_unmerged_pull_request_blocks(self, monkeypatch):
        from orchestrator.services import job_delivery

        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(return_value=_status("closed")),
        )
        reason = await job_delivery.unmerged_pr_block_reason(
            _job(), datasources=[_repo_datasource()]
        )
        assert reason is not None
        assert "closed" in reason.lower()

    async def test_unreachable_forge_blocks_fail_closed(self, monkeypatch):
        """A state that cannot be read is not a merged state."""
        from shared.runtime.services.forge import ForgeError

        from orchestrator.services import job_delivery

        monkeypatch.setattr(
            job_delivery,
            "get_pull_request_status",
            AsyncMock(side_effect=ForgeError("github unreachable")),
        )
        reason = await job_delivery.unmerged_pr_block_reason(
            _job(), datasources=[_repo_datasource()]
        )
        assert reason is not None

    async def test_detached_repository_blocks_fail_closed(self, monkeypatch):
        """The PR is recorded but its connector is gone — the state cannot be
        confirmed, so it must not pass."""
        from orchestrator.services import job_delivery

        monkeypatch.setattr(
            job_delivery, "get_pull_request_status", AsyncMock(return_value=None)
        )
        reason = await job_delivery.unmerged_pr_block_reason(_job(), datasources=[])
        assert reason is not None


# --- The shared gate and its two enforcement points --------------------------

from contextlib import ExitStack  # noqa: E402
from unittest.mock import MagicMock, patch  # noqa: E402

import orchestrator.main  # noqa: E402
from fastapi import HTTPException  # noqa: E402

OWNER_ID = "33333333-3333-3333-3333-333333333333"


def _fake_db(*, can_complete=False, owner_is_admin=False):
    db = MagicMock()
    db.user_can_complete_unmerged_pr = AsyncMock(return_value=can_complete)
    db.resolve_datasources_for_job = AsyncMock(return_value=[_repo_datasource()])
    db.get_user = AsyncMock(return_value={"id": OWNER_ID, "is_admin": owner_is_admin})
    return db


def _pending_job(**overrides):
    job = _job(status="pending_review", user_id=OWNER_ID, project_id=PROJECT_ID)
    job.update(overrides)
    return job


class TestUnmergedPrGateReason:
    async def test_job_without_a_pull_request_costs_no_io(self):
        """No PR record means no grant read and no forge call at all."""
        db = _fake_db()
        with patch("orchestrator.main.app.state.resources.postgres_db", db):
            reason = await control_seams.unmerged_pr_gate_reason(
                _job(pull_request=False), user={"id": OWNER_ID, "is_admin": False}
            )
        assert reason is None
        db.user_can_complete_unmerged_pr.assert_not_awaited()
        db.resolve_datasources_for_job.assert_not_awaited()

    async def test_principal_with_the_grant_is_not_blocked(self):
        db = _fake_db(can_complete=True)
        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", db)
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_delivery.unmerged_pr_block_reason",
                    AsyncMock(return_value="pull request #1 (x/y) is open, not merged"),
                )
            )
            reason = await control_seams.unmerged_pr_gate_reason(
                _pending_job(), user={"id": OWNER_ID, "is_admin": False}
            )
        assert reason is None

    async def test_principal_without_the_grant_is_blocked(self):
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", db)
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_delivery.unmerged_pr_block_reason",
                    AsyncMock(return_value="pull request #1 (x/y) is open, not merged"),
                )
            )
            reason = await control_seams.unmerged_pr_gate_reason(
                _pending_job(), user={"id": OWNER_ID, "is_admin": False}
            )
        assert reason is not None
        assert "not merged" in reason

    async def test_internal_call_uses_the_job_owner_as_principal(self):
        """user=None is the agent/autonomous path; the owner's grants decide."""
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", db)
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_delivery.unmerged_pr_block_reason",
                    AsyncMock(return_value="pull request #1 (x/y) is open, not merged"),
                )
            )
            await control_seams.unmerged_pr_gate_reason(_pending_job(), user=None)
        db.get_user.assert_awaited_once_with(OWNER_ID)
        assert db.user_can_complete_unmerged_pr.await_args.args[0]["id"] == OWNER_ID

    async def test_the_jobs_project_reaches_the_capability_read(self):
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            stack.enter_context(
                patch("orchestrator.main.app.state.resources.postgres_db", db)
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_delivery.unmerged_pr_block_reason",
                    AsyncMock(return_value="blocked"),
                )
            )
            await control_seams.unmerged_pr_gate_reason(
                _pending_job(), user={"id": OWNER_ID, "is_admin": False}
            )
        assert db.user_can_complete_unmerged_pr.await_args.args[1] == PROJECT_ID


class TestApproveJobGate:
    def _patch(self, stack, job, db):
        stack.enter_context(
            patch(
                "orchestrator.security.access.require_internal_or_job_access",
                AsyncMock(return_value=({"id": OWNER_ID, "is_admin": False}, job)),
            )
        )
        stack.enter_context(
            patch.object(
                orchestrator.main.app.state.resources.completion_control_boundary,
                "guard",
                AsyncMock(),
            )
        )
        stack.enter_context(
            patch("orchestrator.main.app.state.resources.postgres_db", db)
        )

    async def test_unmerged_pull_request_refuses_with_403(self):
        job = _pending_job()
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            self._patch(stack, job, db)
            stack.enter_context(
                patch(
                    "orchestrator.services.job_delivery.unmerged_pr_block_reason",
                    AsyncMock(return_value="pull request #1 (x/y) is open, not merged"),
                )
            )
            with pytest.raises(HTTPException) as excinfo:
                await control_seams.approve_job(MagicMock(), "job-1", None)
        assert excinfo.value.status_code == 403
        assert "not merged" in str(excinfo.value.detail)

    async def test_explicit_pr_contract_must_be_proven_before_merge_policy(self):
        job = _pending_job()
        job["context"]["required_deliverables"] = ["pr:knaeckebrothero/kurortengine"]
        db = _fake_db(can_complete=True)
        with ExitStack() as stack:
            self._patch(stack, job, db)
            proof = stack.enter_context(
                patch(
                    "orchestrator.services.deliverable_gate.explicit_pr_delivery_block_reason",
                    AsyncMock(return_value="the recorded PR is missing"),
                )
            )
            merge_policy = stack.enter_context(
                patch(
                    "orchestrator.services.job_controls.JobControlOperations._unmerged_pr_gate_reason",
                    AsyncMock(),
                )
            )
            with pytest.raises(HTTPException) as excinfo:
                await control_seams.approve_job(MagicMock(), "job-1", None)

        assert excinfo.value.status_code == 409
        assert excinfo.value.detail["code"] == "pr_deliverable_unverified"
        proof.assert_awaited_once_with({**job, "id": "job-1"}, db=db)
        merge_policy.assert_not_awaited()

    async def test_proven_pr_still_obeys_existing_open_pr_policy(self):
        job = _pending_job()
        job["context"]["required_deliverables"] = ["pr:knaeckebrothero/kurortengine"]
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            self._patch(stack, job, db)
            stack.enter_context(
                patch(
                    "orchestrator.services.deliverable_gate.explicit_pr_delivery_block_reason",
                    AsyncMock(return_value=None),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_controls.JobControlOperations._unmerged_pr_gate_reason",
                    AsyncMock(return_value="pull request #1 is open, not merged"),
                )
            )
            with pytest.raises(HTTPException) as excinfo:
                await control_seams.approve_job(MagicMock(), "job-1", None)

        assert excinfo.value.status_code == 403
        assert "not merged" in str(excinfo.value.detail)

    async def test_job_without_a_pull_request_is_not_refused(self):
        """Negative control: the overwhelming majority of jobs must be
        completely unaffected by this feature."""
        job = _pending_job()
        job["context"] = {}
        db = _fake_db(can_complete=False)
        with ExitStack() as stack:
            self._patch(stack, job, db)
            stack.enter_context(
                patch.object(
                    orchestrator.main.app.state.resources.completion_control_boundary,
                    "claim",
                    AsyncMock(side_effect=RuntimeError("past the gate")),
                )
            )
            with pytest.raises(HTTPException) as excinfo:
                await control_seams.approve_job(MagicMock(), "job-1", None)
        # The sentinel fires only if execution reached the completion claim,
        # which is past the gate. What matters is that the refusal is not ours.
        assert excinfo.value.status_code != 403


# --- The autonomous seal downgrade (pure decision) ---------------------------


class TestUnmergedPrSealStatus:
    """Pure counterpart of the cloud-diff downgrade it sits beside.

    The I/O half is covered by TestUnmergedPrGateReason; this pins only the
    decision, which is what the seal site inlines.
    """

    async def test_completed_with_an_unmerged_pr_becomes_pending_review(self):
        from orchestrator.services.completion import unmerged_pr_seal_status

        status, action = unmerged_pr_seal_status(
            "completed", loop_id=None, reason="pull request #1 (x/y) is open"
        )
        assert status == "pending_review"
        assert action is not None
        assert "pending_review" in action

    async def test_no_reason_leaves_the_status_alone(self):
        from orchestrator.services.completion import unmerged_pr_seal_status

        assert unmerged_pr_seal_status("completed", loop_id=None, reason=None) == (
            "completed",
            None,
        )

    async def test_a_non_terminal_status_is_never_touched(self):
        from orchestrator.services.completion import unmerged_pr_seal_status

        assert unmerged_pr_seal_status(
            "pending_review", loop_id=None, reason="blocked"
        ) == ("pending_review", None)
        assert unmerged_pr_seal_status("failed", loop_id=None, reason="blocked") == (
            "failed",
            None,
        )

    async def test_loop_jobs_are_excluded(self):
        """Mirrors the cloud-diff downgrade, which excludes loop jobs via
        `not _completion_loop_id`. A loop that stalls produces nothing and
        nobody is watching to unstick it; the loop owns its own delivery."""
        from orchestrator.services.completion import unmerged_pr_seal_status

        assert unmerged_pr_seal_status(
            "completed", loop_id="loop-1", reason="pull request #1 is open"
        ) == ("completed", None)
