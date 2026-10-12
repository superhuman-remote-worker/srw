"""Tests for the orchestrator-side remains of job delegation.

The agent-side delegation path (``delegate_work`` / ``resume_delegation_child``
child jobs, the light ``spawn_subagent`` reader) was deleted in U3 WP4 — the
built-in subagents (``delegate_agent``, ``src/subagents``) replaced it and have
their own tests. What stays here is what survives one release, inert: the
orchestrator's ``waiting`` status for a ``delegation`` freeze, its timeout
sweeper, the cockpit's ``creation_order`` / ``waiting`` fields, and the
``GitManager`` worktree methods the worktree isolation of a subagent child still
uses. See knowledge-base/knowledge/issues/delegation_child_machinery_retirement.md.
"""

import subprocess

import pytest
from orchestrator.application import background_tasks as background_tasks_composition


# ===========================================================================
# TestDetermineJobStatusDelegation — Orchestrator status determination
# ===========================================================================


class TestDetermineJobStatusDelegation:
    """Test determine_job_status() for delegation freeze_type."""

    def test_delegation_freeze_returns_waiting(self):
        from orchestrator.services.completion import determine_job_status

        job = {
            "id": "test-job",
            "config_override": None,
            "context": None,
        }
        result = {
            "should_stop": True,
            "goal_achieved": False,
            "freeze_data": {
                "freeze_type": "delegation",
                "child_job_ids": ["child-1", "child-2"],
            },
        }
        status, note = determine_job_status(job, result)
        assert status == "waiting"

    def test_job_complete_still_works(self):
        """Ensure existing job_complete freeze_type still works."""
        from orchestrator.services.completion import determine_job_status

        job = {
            "id": "test-job",
            "config_override": None,
            "context": None,
        }
        result = {
            "should_stop": True,
            "goal_achieved": True,
            "freeze_data": {"freeze_type": "job_complete"},
        }
        status, note = determine_job_status(job, result)
        # Should be completed or reviewing depending on verification config
        assert status in ("completed", "reviewing")

    def test_vm_upgrade_still_returns_paused(self):
        from orchestrator.services.completion import determine_job_status

        job = {
            "id": "test-job",
            "config_override": None,
            "context": None,
        }
        result = {
            "should_stop": True,
            "goal_achieved": False,
            "freeze_data": {"freeze_type": "vm_upgrade_required"},
        }
        status, note = determine_job_status(job, result)
        assert status == "paused"


# ===========================================================================
# TestGitManagerWorktree — Worktree management in GitManager
# ===========================================================================


class TestGitManagerWorktree:
    """Test GitManager worktree methods using real git repos."""

    @pytest.fixture
    def git_repo(self, tmp_path):
        """Create a real git repo with an initial commit."""
        repo_path = tmp_path / "repo"
        repo_path.mkdir()

        subprocess.run(["git", "init"], cwd=repo_path, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            cwd=repo_path,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            cwd=repo_path,
            capture_output=True,
        )

        # Create an initial commit (required for worktrees)
        (repo_path / "README.md").write_text("# Test Repo")
        subprocess.run(["git", "add", "."], cwd=repo_path, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Initial commit"],
            cwd=repo_path,
            capture_output=True,
        )

        return repo_path

    @pytest.fixture
    def git_manager(self, git_repo):
        from agent.managers.git_manager import GitManager

        return GitManager(git_repo)

    def test_worktree_add_local(self, git_manager, git_repo):
        """Create a worktree on local filesystem."""
        wt_path = ".worktrees/subagent_0"
        assert git_manager.worktree_add(wt_path, "subagent/0")

        # Verify worktree directory exists
        full_path = git_repo / ".worktrees" / "subagent_0"
        assert full_path.exists()
        assert (full_path / "README.md").exists()

    def test_worktree_remove_local(self, git_manager, git_repo):
        """Remove a worktree on local filesystem."""
        wt_path = ".worktrees/subagent_0"
        git_manager.worktree_add(wt_path, "subagent/0")

        assert git_manager.worktree_remove(wt_path)
        full_path = git_repo / ".worktrees" / "subagent_0"
        assert not full_path.exists()

    def test_worktree_list(self, git_manager, git_repo):
        """List worktrees."""
        git_manager.worktree_add(".worktrees/sub_0", "sub/0")
        git_manager.worktree_add(".worktrees/sub_1", "sub/1")

        worktrees = git_manager.worktree_list()
        # Should have at least 3: main + 2 worktrees
        assert len(worktrees) >= 3
        paths = [w.get("path", "") for w in worktrees]
        assert any("sub_0" in p for p in paths)
        assert any("sub_1" in p for p in paths)

    def test_merge_squash(self, git_manager, git_repo):
        """Squash-merge a branch into current HEAD."""
        # Create a worktree and make changes
        git_manager.worktree_add(".worktrees/feature", "feature/test")

        feature_path = git_repo / ".worktrees" / "feature"
        (feature_path / "new_file.txt").write_text("new content")
        subprocess.run(["git", "add", "."], cwd=feature_path, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Add new file"],
            cwd=feature_path,
            capture_output=True,
        )

        # Back on main, squash merge
        success, msg = git_manager.merge_squash("feature/test")
        assert success, f"Merge failed: {msg}"
        assert (git_repo / "new_file.txt").exists()

    def test_delete_branch(self, git_manager, git_repo):
        """Delete a local branch after merge."""
        git_manager.worktree_add(".worktrees/temp", "temp/branch")
        git_manager.worktree_remove(".worktrees/temp")

        # Force delete since branch may not be fully merged
        assert git_manager.delete_branch("temp/branch", force=True)

    def test_merge_squash_no_changes(self, git_manager, git_repo):
        """Squash-merge with no changes succeeds gracefully."""
        git_manager.worktree_add(".worktrees/empty", "empty/branch")

        # Merge without making changes on the branch
        success, msg = git_manager.merge_squash("empty/branch")
        # Should succeed (nothing to merge is fine)
        assert success


# ===========================================================================
# Phase 4: TestDelegationTimeout — Timeout enforcement
# ===========================================================================


class TestDelegationTimeout:
    """The orchestrator keeps the delegation timeout sweeper one release
    (inert without a producer — nothing freezes with ``delegation`` any more)."""

    def test_timeout_sweeper_function_exists(self):
        """Verify the timeout policy and application-owned task wiring."""
        import inspect

        from orchestrator.services import completion_recovery

        assert inspect.iscoroutinefunction(
            completion_recovery.check_delegation_timeouts
        )
        assert inspect.iscoroutinefunction(
            completion_recovery.delegation_timeout_sweeper
        )
        # R1.B12: the background-task composition owns the wiring main.py
        # used to hold.
        tasks_src = inspect.getsource(background_tasks_composition)
        assert "completion_recovery_operations.delegation_timeout_sweeper" in tasks_src
        # R1.B11: started (leader-gated) through the lifecycle's task set and
        # awaited at shutdown under its key.
        start = tasks_src.index(
            '"delegation_timeout"', tasks_src.index("async def start_background_tasks(")
        )
        assert tasks_src.rindex(
            "tasks.start_leader_gated(", 0, start
        ) > tasks_src.rfind("tasks.start(", 0, start)

        assert (
            "delegation_timeout"
            in background_tasks_composition.BACKGROUND_TASK_SHUTDOWN_ORDER
        )


# ===========================================================================
# Phase 3: TestCockpitModels — Verify delegation fields in TypeScript models
# ===========================================================================


class TestCockpitDelegationFields:
    """Verify delegation fields exist in cockpit API models."""

    def test_job_model_has_creation_order(self):
        """Job interface should have creation_order field."""

        with open("cockpit/src/app/core/models/api.model.ts", "r") as f:
            content = f.read()
        assert "creation_order" in content

    def test_job_summary_has_creation_order(self):
        """JobSummary interface should have creation_order field."""
        with open("cockpit/src/app/core/models/audit.model.ts", "r") as f:
            content = f.read()
        assert "creation_order" in content

    def test_waiting_status_exists(self):
        """JobStatus type should include 'waiting'."""
        with open("cockpit/src/app/core/models/api.model.ts", "r") as f:
            content = f.read()
        assert "'waiting'" in content
