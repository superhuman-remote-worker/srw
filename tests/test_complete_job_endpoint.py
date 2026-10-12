"""Tests for the orchestrator complete_job endpoint and verification trigger.

Tests cover:
1. complete_job endpoint: freeze_data persistence, status transitions,
   completed_at, queued_replies cleanup
2. _trigger_verification_on_complete: all 5 guard conditions
3. OrchestratorClient.approve_job: success/failure paths
"""

from tests import b08_completion_helpers as b08_helpers

from unittest.mock import ANY, AsyncMock, MagicMock, patch

import asyncpg
import pytest

from orchestrator.services.completion import (  # noqa: E402
    determine_job_status,
    is_job_completion_freeze,
    is_verification_enabled,
)
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import (
    managed_repository_authority as managed_repository_authority_module,
)


# =============================================================================
# Fixtures
# =============================================================================


def make_job(
    *,
    job_id: str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    status: str = "processing",
    parent_job_id: str | None = None,
    freeze_data: dict | None = None,
    verification_enabled: bool = True,
    autonomy: str = "review",
    config_name: str = "defaults",
    context: dict | None = None,
    config_override: dict | None = None,
) -> dict:
    """Create a minimal job dict for testing."""
    resolved_config = {
        "agent": {
            "agent_id": config_name,
            "autonomy": autonomy,
            "verification": {
                "enabled": verification_enabled,
                "critic_config": "critic",
                "max_rounds": 5,
            },
            "curator": {"enabled": False},
            "scholar": {"enabled": True, "scholar_config": "scholar"},
        },
    }

    job = {
        "id": job_id,
        "status": status,
        "config_name": config_name,
        "parent_job_id": parent_job_id,
        "resolved_config": resolved_config,
        "description": "Test job",
        "context": context or {},
        "project_id": None,
        "freeze_data": freeze_data,
        "config_override": config_override,
    }
    return job


def make_mock_postgres():
    """Create a mock postgres_db with acquire() context manager."""
    mock_db = AsyncMock()

    # Mock the async context manager for acquire()
    mock_conn = AsyncMock()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_db.acquire.return_value = mock_ctx

    return mock_db, mock_conn


# =============================================================================
# complete_job endpoint logic tests
# =============================================================================


class TestCompleteJobFreezeData:
    """Test freeze_data persistence in complete_job flow."""

    def test_freeze_data_stored_in_job_dict(self):
        """When freeze_data is in the result, it should be set on the job dict."""
        job = make_job()
        result = {
            "should_stop": True,
            "goal_achieved": False,
            "freeze_data": {"freeze_type": "job_complete", "summary": "done"},
        }

        # Simulate the orchestrator's freeze_data assignment
        if result.get("freeze_data"):
            job["freeze_data"] = result["freeze_data"]

        assert job["freeze_data"]["freeze_type"] == "job_complete"
        assert job["freeze_data"]["summary"] == "done"

    def test_freeze_data_enables_completion_check(self):
        """After freeze_data is set on job, is_job_completion_freeze should return True."""
        job = make_job()
        assert is_job_completion_freeze(job) is False

        # Simulate orchestrator writing freeze_data
        job["freeze_data"] = {"freeze_type": "job_complete", "summary": "done"}
        assert is_job_completion_freeze(job) is True

    def test_phase_boundary_freeze_not_completion(self):
        """Phase boundary freeze should NOT be treated as job completion."""
        job = make_job(freeze_data={"freeze_type": "phase_boundary", "phase_number": 3})
        assert is_job_completion_freeze(job) is False


class TestCompleteJobStatusDetermination:
    """Test status transitions in complete_job flow."""

    def test_completed_job_gets_reviewing_with_verification(self):
        """Job completion + verification enabled → reviewing."""
        job = make_job(
            verification_enabled=True,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "reviewing"

    def test_completed_job_gets_completed_without_verification(self):
        """Job completion + verification disabled + goal achieved → completed."""
        job = make_job(verification_enabled=False)
        result = {"should_stop": True, "goal_achieved": True}
        status, err = determine_job_status(job, result)
        assert status == "completed"

    def test_frozen_job_gets_pending_review(self):
        """Job frozen (no verification, not goal_achieved) → pending_review."""
        job = make_job(
            verification_enabled=False,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, err = determine_job_status(job, result)
        assert status == "pending_review"

    def test_error_gets_failed(self):
        """Job with error → failed."""
        job = make_job()
        result = {"error": {"message": "crashed"}}
        status, err = determine_job_status(job, result)
        assert status == "failed"
        assert err == "crashed"

    def test_completed_at_only_for_completed(self):
        """completed_at should only be set when status is 'completed'."""
        # This tests the logic, not the DB write
        job = make_job(verification_enabled=True)
        result = {"should_stop": True, "goal_achieved": True}
        status, _ = determine_job_status(job, result)
        assert status == "reviewing"  # Not "completed", so no completed_at

        job2 = make_job(verification_enabled=False)
        result2 = {"should_stop": True, "goal_achieved": True}
        status2, _ = determine_job_status(job2, result2)
        assert status2 == "completed"  # This one gets completed_at


class TestMemoryUnavailableStatus:
    """memory/KB-unavailable freeze → bounded pause-then-fail.

    knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md
    """

    def _result(self, freeze_type="memory_unavailable"):
        return {
            "should_stop": True,
            "freeze_data": {
                "freeze_type": freeze_type,
                "reason": "Embedding service unavailable at startup.",
            },
        }

    def test_first_attempt_pauses(self):
        job = make_job(verification_enabled=False, context={})
        status, err = determine_job_status(job, self._result())
        assert status == "paused"
        assert err is None

    def test_under_cap_pauses(self):
        job = make_job(verification_enabled=False, context={"memory_retry_count": 1})
        status, _ = determine_job_status(job, self._result())
        assert status == "paused"

    def test_at_cap_fails_with_reason(self):
        job = make_job(verification_enabled=False, context={"memory_retry_count": 2})
        status, err = determine_job_status(job, self._result())
        assert status == "failed"
        assert err is not None
        assert "embedding" in err.lower()

    def test_kb_unavailable_pauses_then_fails(self):
        result = self._result(freeze_type="kb_unavailable")
        assert (
            determine_job_status(
                make_job(verification_enabled=False, context={}), result
            )[0]
            == "paused"
        )
        assert (
            determine_job_status(
                make_job(verification_enabled=False, context={"memory_retry_count": 2}),
                result,
            )[0]
            == "failed"
        )

    def test_context_as_json_string(self):
        # The job row's context can arrive as a JSON string, not a dict.
        job = make_job(verification_enabled=False)
        job["context"] = '{"memory_retry_count": 2}'
        status, _ = determine_job_status(job, self._result())
        assert status == "failed"


class TestCompleteJobCriticStatus:
    """Test status determination for critic (sub) jobs."""

    def test_critic_approved_gets_completed(self):
        """Critic with approved verdict → completed."""
        job = make_job(
            parent_job_id="parent-123",
            freeze_data={
                "status": "completed",
                "freeze_type": "verdict",
                "verdict": "approved",
            },
        )
        result = {"should_stop": True, "goal_achieved": True}
        status, _ = determine_job_status(job, result)
        assert status == "completed"

    def test_critic_returned_gets_waiting(self):
        """Critic with returned verdict → waiting."""
        job = make_job(
            parent_job_id="parent-123",
            freeze_data={
                "status": "waiting",
                "freeze_type": "verdict",
                "verdict": "returned",
            },
        )
        result = {"should_stop": True, "goal_achieved": False}
        status, _ = determine_job_status(job, result)
        assert status == "waiting"

    def test_critic_normalizes_job_completed(self):
        """Critic with status='job_completed' → normalized to 'completed'."""
        job = make_job(
            parent_job_id="parent-123",
            freeze_data={"status": "job_completed"},
        )
        result = {"should_stop": True, "goal_achieved": True}
        status, _ = determine_job_status(job, result)
        assert status == "completed"

    def test_critic_no_freeze_data_infers_from_goal(self):
        """Critic without freeze_data: goal_achieved=True → completed."""
        job = make_job(parent_job_id="parent-123")
        result = {"should_stop": True, "goal_achieved": True}
        status, _ = determine_job_status(job, result)
        assert status == "completed"

    def test_critic_no_freeze_data_no_goal_pending(self):
        """Critic without freeze_data: goal_achieved=False → pending_review."""
        job = make_job(parent_job_id="parent-123")
        result = {"should_stop": True, "goal_achieved": False}
        status, _ = determine_job_status(job, result)
        assert status == "pending_review"


# =============================================================================
# Verification trigger guard tests
# =============================================================================


class TestVerificationTriggerGuards:
    """Test the guard conditions in ``_trigger_verification_on_complete``.

    This class previously asserted the guard *predicates* directly (e.g.
    ``assert result.get("error") is not None``) without ever calling the
    guarded function — it would have kept passing even if every guard were
    deleted from ``_trigger_verification_on_complete``. Every case below
    instead invokes the REAL function (mocked ``postgres_db`` only) and
    asserts no critic was created: ``create_job`` is never awaited and
    ``actions`` stays empty.

    Each job/result starts from ``_passing_job``/``_passing_result`` — a
    baseline that clears every guard — with only the ONE condition under
    test broken, so a case only goes green because of the guard it names, not
    because some other guard also happened to fire.
    ``test_critic_created_when_all_guards_pass`` is the positive control that
    proves the untouched baseline really does reach ``create_job``; without
    it, an implementation that always returns early (e.g. every guard
    accidentally inverted at once) would pass every case above for the wrong
    reason.
    """

    @staticmethod
    def _passing_job(**overrides) -> dict:
        base: dict = dict(
            verification_enabled=True,
            status="reviewing",
            freeze_data={
                "freeze_type": "job_complete",
                "summary": "done",
                "deliverables": [],
                "confidence": 0.9,
            },
        )
        base.update(overrides)
        return make_job(**base)

    @staticmethod
    def _passing_result(**overrides) -> dict:
        base = {"should_stop": True, "goal_achieved": True}
        base.update(overrides)
        return base

    @pytest.mark.asyncio
    async def test_no_critic_created_when_result_has_error(self, monkeypatch):
        """Guard 1: an errored result must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job()
        result = self._passing_result(error={"message": "failed"})
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_no_critic_created_when_not_stopped(self, monkeypatch):
        """Guard 2: a job that hasn't stopped must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job()
        result = self._passing_result(should_stop=False)
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_no_critic_created_when_subjob(self, monkeypatch):
        """Guard 3: sub-jobs (parent_job_id set) must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job(parent_job_id="parent-123")
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_no_critic_created_when_lite_backend(self, monkeypatch):
        """Guard 4: a lite (virtual/none) workspace backend has no git
        workspace for the critic handoff, so it must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job(config_override={"workspace": {"backend": "virtual"}})
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_no_critic_created_when_verification_disabled(self, monkeypatch):
        """Guard 5: verification disabled must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job(verification_enabled=False)
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_no_critic_created_when_not_job_completion_freeze(self, monkeypatch):
        """Guard 6: a phase-boundary freeze (not a genuine job completion,
        and status isn't 'reviewing' either) must not spawn a critic."""
        from orchestrator import main

        create_job_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )

        job = self._passing_job(
            status="processing",
            freeze_data={"freeze_type": "phase_boundary", "phase_number": 3},
        )
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_critic_created_when_all_guards_pass(self, monkeypatch):
        """Positive control: the SAME baseline used above, left untouched,
        really does reach ``create_job``."""
        from orchestrator import main

        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        # No critic already in flight. Stubbed rather than left real because
        # the baseline job id is not a UUID and the guard fails CLOSED on one.
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )

        job = self._passing_job()
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_awaited_once()
        assert any("critic job" in a and "created" in a for a in actions)

    @pytest.mark.asyncio
    async def test_critic_does_not_copy_parent_runtime_authority(self, monkeypatch):
        """The child inherits by parent reference, not by owning its runtime."""
        from orchestrator import main

        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )

        runtime = {
            "provisioner": "k8s",
            "status": "ready",
            "_runtime_incarnation": "11111111-2222-4333-8444-555555555555",
        }
        job = self._passing_job(
            context={"workspace_container": runtime},
            config_override={"workspace": {"backend": "sandbox"}},
        )

        await b08_helpers.trigger_verification_on_complete(
            job, self._passing_result(), []
        )

        kwargs = create_job_mock.call_args.kwargs
        assert kwargs["context"]["inherits_parent_workspace"] is True
        assert "workspace_container" not in kwargs["context"]
        assert "vm" not in kwargs["context"]
        assert kwargs["workspace_assignment_source"] == "parent_inheritance"
        assert "authoritative_workspace_context" not in kwargs

    @pytest.mark.asyncio
    async def test_no_critic_created_when_one_is_already_in_flight(self, monkeypatch):
        """Guard 7: the same baseline, with a live critic already spawned for
        this target — a retried /complete must not double it."""
        from orchestrator import main

        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        round_lookup_mock = AsyncMock()
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_verification_critic_for_round",
            round_lookup_mock,
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=True),
        )

        job = self._passing_job()
        result = self._passing_result()
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(job, result, actions)

        create_job_mock.assert_not_awaited()
        round_lookup_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_durable_replay_finishes_existing_critic_handoff(self, monkeypatch):
        """A post-INSERT replay reuses the indexed critic and finishes S30."""
        from orchestrator import main

        critic_id = "11111111-2222-3333-4444-555555555555"
        create_job_mock = AsyncMock()
        # The indexed critic row carries its OWN context, including the
        # inherits_parent_workspace flag stamped at spawn. That flag, not the
        # parent's context, decides the worktree path.
        round_lookup_mock = AsyncMock(
            return_value={
                "id": critic_id,
                "config_name": "critic",
                "context": {"inherits_parent_workspace": True},
            }
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_verification_critic_for_round",
            round_lookup_mock,
        )
        bind_repo = AsyncMock(return_value=True)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "bind_job_managed_repository",
            bind_repo,
        )
        # The handoff branches from the parent's PROVEN authority, not from a
        # repo name guessed off the job id, so the resolver is the collaborator
        # under stub here.
        authority = AsyncMock(
            return_value={
                "repo_name": "job-aaaaaaaa",
                "clean_repo_url": "http://gitea/job-aaaaaaaa.git",
            }
        )
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
            authority,
        )

        conn = AsyncMock()
        conn.execute.return_value = "UPDATE 1"
        acquired = MagicMock()
        acquired.__aenter__ = AsyncMock(return_value=conn)
        acquired.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "acquire",
            MagicMock(return_value=acquired),
        )

        gitea = MagicMock()
        gitea.is_initialized = True
        gitea.create_branch = AsyncMock(return_value=True)
        monkeypatch.setattr(main.app.state.resources, "gitea_client", gitea)
        trigger_dispatch = MagicMock()
        monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", trigger_dispatch)

        job = self._passing_job(
            context={
                "git_remote_url": "http://gitea/job-aaaaaaaa.git",
                "workspace_container": {"status": "ready"},
            }
        )
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(
            job,
            self._passing_result(),
            actions,
            reconcile_existing_critic=True,
        )

        create_job_mock.assert_not_awaited()
        round_lookup_mock.assert_awaited_once_with(job["id"], 0)
        gitea.create_branch.assert_awaited_once_with(
            "job-aaaaaaaa",
            "subjob/11111111/critic",
            from_branch="main",
        )
        bind_repo.assert_awaited_once_with(
            critic_id,
            repo_name="job-aaaaaaaa",
            clean_url="http://gitea/job-aaaaaaaa.git",
        )
        update_args = conn.execute.await_args.args
        assert update_args[1:] == (
            "subjob/11111111/critic",
            "/home/agent-host/workspace/worktrees/11111111-critic",
            critic_id,
        )
        trigger_dispatch.assert_called_once_with(dependencies=ANY)
        assert actions == [f"critic job {critic_id} reconciled"]

    @pytest.mark.asyncio
    async def test_replay_leaves_worktree_null_for_a_non_inheriting_critic(
        self, monkeypatch
    ):
        """A critic without the inherit discriminator gets no worktree.

        The parent still carries a workspace_container here — presence of that
        key on the PARENT is exactly the ambiguous signal this handoff must not
        gate on. A stateless-lane critic provisions its own workspace, so a
        parent-derived worktree path would point at a directory that never
        exists on its host.
        """
        from orchestrator import main

        critic_id = "11111111-2222-3333-4444-555555555555"
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", AsyncMock()
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "get_verification_critic_for_round",
            AsyncMock(
                return_value={
                    "id": critic_id,
                    "config_name": "critic",
                    "context": {"verification_target": "aaaaaaaa"},
                }
            ),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "bind_job_managed_repository",
            AsyncMock(return_value=True),
        )
        monkeypatch.setattr(
            managed_repository_authority_module,
            "prepare_job_primary_repository_authority",
            AsyncMock(
                return_value={
                    "repo_name": "job-aaaaaaaa",
                    "clean_repo_url": "http://gitea/job-aaaaaaaa.git",
                }
            ),
        )

        conn = AsyncMock()
        conn.execute.return_value = "UPDATE 1"
        acquired = MagicMock()
        acquired.__aenter__ = AsyncMock(return_value=conn)
        acquired.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "acquire",
            MagicMock(return_value=acquired),
        )

        gitea = MagicMock()
        gitea.is_initialized = True
        gitea.create_branch = AsyncMock(return_value=True)
        monkeypatch.setattr(main.app.state.resources, "gitea_client", gitea)
        monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", MagicMock())

        job = self._passing_job(
            context={
                "git_remote_url": "http://gitea/job-aaaaaaaa.git",
                "workspace_container": {"status": "ready"},
            }
        )
        actions: list[str] = []

        await b08_helpers.trigger_verification_on_complete(
            job,
            self._passing_result(),
            actions,
            reconcile_existing_critic=True,
        )

        update_args = conn.execute.await_args.args
        assert update_args[1:] == (
            "subjob/11111111/critic",
            None,
            critic_id,
        )
        assert actions == [f"critic job {critic_id} reconciled"]

    @pytest.mark.asyncio
    async def test_index_loser_skips_critic_side_effects(self, monkeypatch):
        """The unique index, not the optimistic live-critic read, owns races."""
        from orchestrator import main

        violation = asyncpg.UniqueViolationError("duplicate critic round")
        violation.constraint_name = "jobs_verification_uniq"
        create_job_mock = AsyncMock(side_effect=violation)
        monkeypatch.setattr(
            main.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        trigger_dispatch = MagicMock()
        monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", trigger_dispatch)

        job = self._passing_job()
        actions: list[str] = []
        await b08_helpers.trigger_verification_on_complete(
            job, self._passing_result(), actions
        )

        create_job_mock.assert_awaited_once()
        trigger_dispatch.assert_not_called()
        assert actions == [
            f"critic round 0 already exists for {job['id']} — spawn skipped"
        ]

    @pytest.mark.asyncio
    async def test_unrelated_unique_violation_still_raises(self, monkeypatch):
        """Only the exact critic-index loser is a successful dedupe."""
        from orchestrator import main

        violation = asyncpg.UniqueViolationError("other duplicate")
        violation.constraint_name = "some_other_unique_index"
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(side_effect=violation),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            main.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )

        with pytest.raises(asyncpg.UniqueViolationError) as exc_info:
            await b08_helpers.trigger_verification_on_complete(
                self._passing_job(), self._passing_result(), []
            )
        assert exc_info.value.constraint_name == "some_other_unique_index"


class TestVerificationTriggerIntegration:
    """Test the full flow: determine_job_status → verification trigger eligibility."""

    def test_status_reviewing_enables_verification_trigger(self):
        """When determine_job_status returns 'reviewing', the verification
        trigger guard should pass (status == 'reviewing')."""
        job = make_job(
            verification_enabled=True,
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": True}

        # Step 1: determine_job_status
        new_status, _ = determine_job_status(job, result)
        assert new_status == "reviewing"

        # Step 2: simulate in-memory update (like the endpoint does)
        job["status"] = new_status

        # Step 3: verification trigger guards should all pass
        assert not result.get("error")
        assert result.get("should_stop", False)
        assert job.get("parent_job_id") is None
        assert is_verification_enabled(job)
        assert job.get("status") == "reviewing"

    def test_pending_review_without_freeze_blocks_verification(self):
        """When job is pending_review without job_complete freeze_data,
        verification should NOT trigger (this was the original bug)."""
        job = make_job(
            verification_enabled=True,
            # No freeze_data — just a phase boundary stop
        )
        result = {"should_stop": True, "goal_achieved": False}

        new_status, _ = determine_job_status(job, result)
        assert new_status == "pending_review"

        job["status"] = new_status

        # Guard 5 should block: no job_complete freeze and status != reviewing
        assert not is_job_completion_freeze(job)
        assert job.get("status") != "reviewing"
        # → verification should NOT trigger

    def test_goal_achieved_with_freeze_data_triggers_verification(self):
        """Goal achieved + freeze_data + verification enabled → triggers."""
        job = make_job(
            verification_enabled=True,
            freeze_data={"freeze_type": "job_complete", "summary": "all done"},
        )
        result = {"should_stop": True, "goal_achieved": True}

        new_status, _ = determine_job_status(job, result)
        assert new_status == "reviewing"

        job["status"] = new_status

        # All guards pass
        assert is_job_completion_freeze(job) or job.get("status") == "reviewing"


# =============================================================================
# OrchestratorClient.approve_job tests
# =============================================================================


class TestOrchestratorClientApproveJob:
    """Test the approve_job client method."""

    @pytest.fixture
    def client(self):
        from agent.api.orchestrator_client import OrchestratorClient

        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.1",
            pod_port=8001,
            hostname="test-agent",
            config_name="default",
        )

    @pytest.mark.asyncio
    async def test_approve_success(self, client):
        """Successful approval returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.put = AsyncMock(return_value=mock_response)
            result = await client.approve_job("job-123")

        assert result is True
        mock_http.put.assert_called_once()
        call_url = mock_http.put.call_args[0][0]
        assert "job-123" in call_url
        assert "/approve" in call_url

    @pytest.mark.asyncio
    async def test_approve_with_notes(self, client):
        """Notes are passed in the request body."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.put = AsyncMock(return_value=mock_response)
            await client.approve_job("job-123", notes="looks good")

        call_kwargs = mock_http.put.call_args[1]
        assert call_kwargs["json"]["notes"] == "looks good"

    @pytest.mark.asyncio
    async def test_approve_failure(self, client):
        """Non-200 response returns False."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.put = AsyncMock(return_value=mock_response)
            result = await client.approve_job("job-123")

        assert result is False

    @pytest.mark.asyncio
    async def test_approve_connection_error(self, client):
        """Connection errors return False gracefully."""
        import httpx

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.put = AsyncMock(
                side_effect=httpx.RequestError("Connection refused")
            )
            result = await client.approve_job("job-123")

        assert result is False

    @pytest.mark.asyncio
    async def test_approve_auto_connects(self, client):
        """Client auto-connects if _client is None."""
        mock_response = MagicMock()
        mock_response.status_code = 200

        # _client starts as None
        assert client._client is None

        with patch.object(client, "connect", AsyncMock()) as mock_connect:
            # After connect, set _client
            async def set_client():
                client._client = AsyncMock()
                client._client.put = AsyncMock(return_value=mock_response)

            mock_connect.side_effect = set_client

            result = await client.approve_job("job-123")

        mock_connect.assert_called_once()
        assert result is True


# =============================================================================
# OrchestratorClient.report_completion tests
# =============================================================================


class TestOrchestratorClientReportCompletion:
    """Test that report_completion sends the correct payload."""

    @pytest.fixture
    def client(self):
        from agent.api.orchestrator_client import OrchestratorClient

        return OrchestratorClient(
            orchestrator_url="http://localhost:8085",
            pod_ip="10.0.0.1",
            pod_port=8001,
            hostname="test-agent",
            config_name="default",
        )

    @pytest.mark.asyncio
    async def test_sends_freeze_data(self, client):
        """Freeze data from the graph result is included in the payload."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "reviewing", "actions": []}

        result = {
            "should_stop": True,
            "goal_achieved": True,
            "freeze_data": {"freeze_type": "job_complete", "summary": "done"},
        }

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.report_completion("job-123", result)

        call_kwargs = mock_http.post.call_args[1]
        payload = call_kwargs["json"]
        assert payload["should_stop"] is True
        assert payload["goal_achieved"] is True
        assert payload["freeze_data"]["freeze_type"] == "job_complete"
        assert "lease_token" not in payload
        assert "agent_id" not in payload
        assert "client_report_id" not in payload
        assert call_kwargs["timeout"] == 60.0

    @pytest.mark.asyncio
    async def test_stateless_completion_sends_token_with_wide_timeout(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "completed", "actions": []}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.report_completion(
                "job-123", {"should_stop": True}, lease_token=17
            )

        call_kwargs = mock_http.post.call_args.kwargs
        assert call_kwargs["json"]["lease_token"] == 17
        assert call_kwargs["timeout"] == 300.0

    @pytest.mark.asyncio
    async def test_sends_optional_pinned_fence_and_report_identity(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "completed", "actions": []}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.report_completion(
                "job-123",
                {"should_stop": True},
                agent_id="22222222-2222-4222-8222-222222222222",
                client_report_id="11111111-1111-4111-8111-111111111111",
            )

        payload = mock_http.post.call_args.kwargs["json"]
        assert payload["agent_id"] == "22222222-2222-4222-8222-222222222222"
        assert payload["client_report_id"] == ("11111111-1111-4111-8111-111111111111")
        assert "lease_token" not in payload

    @pytest.mark.asyncio
    async def test_retry_uses_checkpointed_four_field_payload_verbatim(self, client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "completed", "actions": []}
        checkpointed = {
            "should_stop": True,
            "goal_achieved": True,
            "error": None,
            "freeze_data": {
                "freeze_type": "job_complete",
                "generated_at": "2026-08-12T22:00:00Z",
            },
        }
        result = {
            "client_report_id": "11111111-1111-4111-8111-111111111111",
            "completion_report_payload": checkpointed,
            # These live values deliberately disagree with the durable stop.
            "should_stop": False,
            "goal_achieved": False,
            "error": {"message": "later mutation"},
            "freeze_data": None,
        }

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.report_completion("job-123", result, lease_token=17)

        payload = mock_http.post.call_args.kwargs["json"]
        assert {
            key: value
            for key, value in payload.items()
            if key not in {"lease_token", "client_report_id"}
        } == checkpointed
        assert payload["client_report_id"] == result["client_report_id"]
        assert payload["lease_token"] == 17

    @pytest.mark.asyncio
    async def test_sends_error(self, client):
        """Error results are forwarded to the orchestrator."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "failed", "actions": []}

        result = {"error": {"message": "OOM"}}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            await client.report_completion("job-123", result)

        call_kwargs = mock_http.post.call_args[1]
        payload = call_kwargs["json"]
        assert payload["error"]["message"] == "OOM"
        assert payload["should_stop"] is False  # default

    @pytest.mark.asyncio
    async def test_returns_true_on_success(self, client):
        """Returns True when orchestrator handles completion."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"new_status": "completed", "actions": []}

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            success = await client.report_completion("job-123", {"should_stop": True})

        assert success is True

    @pytest.mark.asyncio
    async def test_returns_true_on_async_accept(self, client):
        """HTTP 202 is a successful durable accept for stateless finalization."""
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.json.side_effect = ValueError("empty response body")

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            success = await client.report_completion(
                "job-123", {"should_stop": True}, lease_token=17
            )

        assert success is True

    @pytest.mark.asyncio
    async def test_machine_coded_nonterminal_422_is_definitive(self, client):
        from agent.api.orchestrator_client import CompletionNonTerminalReportError

        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = {
            "detail": {
                "code": "completion_non_terminal_report",
                "message": "stateless completion requires should_stop=true",
            }
        }

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            with pytest.raises(CompletionNonTerminalReportError) as caught:
                await client.report_completion(
                    "job-123", {"should_stop": True}, lease_token=17
                )

        assert caught.value.code == "completion_non_terminal_report"
        assert caught.value.message == (
            "stateless completion requires should_stop=true"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "detail",
        [
            "client_report_id was reused",
            {"code": "different_422", "message": "not this contract"},
            {"message": "missing machine code"},
        ],
    )
    async def test_other_422_remains_ambiguous_false(self, client, detail):
        mock_response = MagicMock()
        mock_response.status_code = 422
        mock_response.json.return_value = {"detail": detail}
        mock_response.text = "unprocessable"

        with patch.object(client, "_client", AsyncMock()) as mock_http:
            mock_http.post = AsyncMock(return_value=mock_response)
            success = await client.report_completion(
                "job-123", {"should_stop": True}, lease_token=17
            )

        assert success is False


# =============================================================================
# Structured cooldown fail-fast error (knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md)
# =============================================================================


class TestStructuredCooldownFailfast:
    """The fail-fast error dict gained classification/model/reset_at — the
    completion path must neither divert on the extra keys nor lose the text."""

    def test_cooldown_failfast_dict_fails_with_message_text(self):
        job = make_job()
        result = {
            "should_stop": True,
            "goal_achieved": False,
            "error": {
                "message": "cooldown msg",
                "type": "llm_error",
                "recoverable": False,
                "classification": "cooldown",
                "model": "gpt-5.3-codex-spark",
                "reset_at": 1785412444.0,
            },
        }
        new_status, error_message = determine_job_status(job, result)
        assert new_status == "failed"
        assert error_message == "cooldown msg"

    @pytest.mark.asyncio
    async def test_update_job_status_writes_error_details(self):
        from contextlib import asynccontextmanager

        from orchestrator.database.postgres import PostgresDB

        with patch.dict("os.environ", {"DATABASE_URL": "postgresql://test"}):
            db = PostgresDB()
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")

        @asynccontextmanager
        async def fake_acquire():
            yield conn

        db.acquire = fake_acquire

        updated = await db.update_job_status(
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            status="failed",
            error_message="cooldown msg",
            error_details={"classification": "cooldown", "reset_at": 1785412444.0},
        )
        assert updated is True
        query = conn.execute.await_args.args[0]
        params = conn.execute.await_args.args[1:]
        assert "error_details = $" in query
        assert "::jsonb" in query
        assert any(
            isinstance(p, str) and '"classification": "cooldown"' in p for p in params
        )
