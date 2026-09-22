"""A job-ending push that does not land must be recorded, not swallowed.

`GitManager.push()` returns False on failure, and every job-ending caller in
`src/core/phase.py` used to discard it. A job whose deliverables never left the
pod therefore finished indistinguishable from one that delivered cleanly — it
reported success at confidence 1.0 and the pod was reclaimed with the only copy
of the work.

That is not hypothetical. A parser regression made *every* push fail for a
whole job, 26 times, and the incident was invisible for a day:
knowledge-history/done/git_push_fails_silently_via_workspace_backend.md (dev job
`40efbb39`). The push bug itself is fixed; this pins the consequence-handling,
so the next such regression is loud even though the cause will be different.

The push is deliberately NOT retried here — `push()` reports its own reason and
the pod is going away regardless. What matters is that the failure reaches the
freeze record the orchestrator stores, so the critic, the deliverable gate and
the cockpit can tell "empty because delivery failed" from "empty because the
agent produced nothing".
"""

import logging
import shutil
import subprocess
from unittest.mock import MagicMock

import pytest

from shared.runtime.core.loader import AgentConfig  # noqa: E402
from agent.core.phase import (  # noqa: E402
    DELIVERED_COMMIT_KEY,
    DELIVERY_ERROR_KEY,
    DELIVERY_FAILED_KEY,
    finalize_job,
    freeze_for_review,
)
from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig  # noqa: E402
from agent.managers.git_manager import GitManager  # noqa: E402
from agent.tools.core.job import (  # noqa: E402
    clear_final_phase_data,
    seed_final_phase_data,
)
from tests._fs_backend import FilesystemTestBackend  # noqa: E402


def make_config(autonomy: str = "partial") -> AgentConfig:
    return AgentConfig(agent_id="test", display_name="Test", autonomy=autonomy)


def make_state(job_id: str = "test-job", phase_number: int = 1) -> dict:
    return {
        "job_id": job_id,
        "phase_number": phase_number,
        "is_strategic_phase": True,
        "messages": [],
    }


def make_workspace(*, pushed: bool, has_remote: bool = True) -> MagicMock:
    """A workspace whose git manager pushes (or doesn't) as instructed.

    After a push that landed, the tree reads clean and fully pushed — the
    state the seal verifies before it trusts the branch tip.
    """
    git = MagicMock()
    git.is_active = True
    git.has_remote = MagicMock(return_value=has_remote)
    git.push = MagicMock(return_value=pushed)
    git.push_ref = MagicMock(return_value=pushed)
    git.tag = MagicMock(return_value=True)
    git.commit = MagicMock(return_value=True)
    git.uncommitted_paths = MagicMock(return_value=[])
    git.has_unpushed_commits = MagicMock(return_value=not pushed)
    git.get_current_commit = MagicMock(return_value="abc1234")

    ws = MagicMock()
    ws.git_manager = git
    ws.get_head_commit = MagicMock(return_value="abc1234")
    return ws


def seed_final_data(job_id: str = "test-job") -> None:
    seed_final_phase_data(
        job_id,
        {
            "summary": "done",
            "deliverables": ["output/report.md"],
            "confidence": 1.0,
            "job_id": job_id,
        },
    )


class TestFinalizeJobRecordsDeliveryFailure:
    def setup_method(self):
        clear_final_phase_data("test-job")

    def teardown_method(self):
        clear_final_phase_data("test-job")

    def test_full_autonomy_completion_records_a_failed_push(self, caplog):
        """The autonomy=full branch: reports success, so it must report this."""
        seed_final_data()
        ws = make_workspace(pushed=False)

        with caplog.at_level(logging.ERROR):
            result = finalize_job(
                make_state(), ws, MagicMock(), config=make_config("full")
            )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True
        assert result.freeze_data[DELIVERY_ERROR_KEY]
        # Loud, not a warning buried among push chatter.
        assert any(r.levelno >= logging.ERROR for r in caplog.records)

    def test_freeze_branch_records_a_failed_push(self, caplog):
        """The non-full 'freeze for review' branch."""
        seed_final_data()
        ws = make_workspace(pushed=False)

        with caplog.at_level(logging.ERROR):
            result = finalize_job(
                make_state(), ws, MagicMock(), config=make_config("partial")
            )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True

    def test_successful_push_leaves_no_marker(self):
        """Absence means delivered; nothing writes the key False."""
        seed_final_data()
        ws = make_workspace(pushed=True)

        result = finalize_job(make_state(), ws, MagicMock(), config=make_config("full"))

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        assert DELIVERY_ERROR_KEY not in result.freeze_data

    def test_no_remote_is_not_a_delivery_failure(self):
        """push() also returns False when no remote is configured.

        Conflating "nothing to deliver to" with "delivery failed" would mark
        every remote-less job as lost — a false alarm on a legitimate
        configuration, which is worse than the silence being replaced.
        """
        seed_final_data()
        ws = make_workspace(pushed=False, has_remote=False)

        result = finalize_job(make_state(), ws, MagicMock(), config=make_config("full"))

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        ws.git_manager.push.assert_not_called()

    def test_inactive_git_is_not_a_delivery_failure(self):
        """Git versioning off is a configuration, not a lost deliverable."""
        seed_final_data()
        ws = make_workspace(pushed=False)
        ws.git_manager.is_active = False

        result = finalize_job(make_state(), ws, MagicMock(), config=make_config("full"))

        assert DELIVERY_FAILED_KEY not in result.freeze_data


class TestFreezeForReviewRecordsDeliveryFailure:
    def test_boundary_freeze_records_a_failed_push(self):
        ws = make_workspace(pushed=False)

        result = freeze_for_review(
            make_state(),
            ws,
            MagicMock(),
            phase_type="strategic",
            phase_number=1,
        )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True

    def test_boundary_freeze_clean_push_leaves_no_marker(self):
        ws = make_workspace(pushed=True)

        result = freeze_for_review(
            make_state(),
            ws,
            MagicMock(),
            phase_type="strategic",
            phase_number=1,
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data


# =============================================================================
# The seal must not pin a stale revision
# =============================================================================
#
# knowledge-base/knowledge/issues/worker_git_versioning_stops_midjob_seal_pins_stale_revision.md
# (VM job afa0004b): versioning stopped committing mid-job, the job-ending
# commit never landed, and the push that followed reported success — "Everything
# up-to-date" — because there was nothing NEW to push. The branch tip was two
# hours old, the freeze carried ``head_commit: None``, and the deliverable gate
# passed on the scaffolds sitting at that tip. A push that lands proves only
# that the remote holds HEAD; it says nothing about whether HEAD holds the work.


def _git(cwd, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


@pytest.fixture
def delivery_repo(tmp_path):
    """A workspace cloned from (and pushed to) a real bare remote.

    Returns ``(workspace, root, remote)``. The workspace is configured to
    deliver to that remote — the job-repo shape every dispatched job has.
    """
    remote = tmp_path / "remote.git"
    root = tmp_path / "workspace"
    root.mkdir()
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(remote))
    _git(root, "init", "-q", "-b", "main")
    _git(root, "config", "user.email", "agent@test.local")
    _git(root, "config", "user.name", "Agent")
    (root / "output").mkdir()
    (root / "output" / "report.md").write_text("scaffold: not yet measured\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "scaffold")
    _git(root, "remote", "add", "origin", str(remote))
    _git(root, "push", "-q", "-u", "origin", "main")

    ws = WorkspaceManager(
        job_id="test-job",
        config=WorkspaceManagerConfig(
            structure=["output/"],
            git_versioning=True,
            git_remote_url=str(remote),
        ),
        base_path=root,
        backend=FilesystemTestBackend(root),
    )
    ws._git_manager = GitManager(root, backend=ws.backend)
    ws._initialized = True
    return ws, root, remote


def _remote_head(remote) -> str:
    return _git(remote, "rev-parse", "main")


def _remote_file(remote, path: str) -> str:
    return _git(remote, "show", f"main:{path}")


@pytest.mark.skipif(shutil.which("git") is None, reason="git not available")
class TestSealRefusesAStaleRevision:
    def setup_method(self):
        clear_final_phase_data("test-job")

    def teardown_method(self):
        clear_final_phase_data("test-job")

    @pytest.mark.parametrize("autonomy", ["partial", "full"])
    def test_clean_pushed_tree_seals_as_before(self, delivery_repo, autonomy):
        """Contrast: a job whose work is committed and pushed seals clean,
        and the record names the exact commit the remote now holds."""
        ws, root, remote = delivery_repo
        (root / "output" / "report.md").write_text("final report\n")
        _git(root, "commit", "-q", "-am", "final report")
        _git(root, "push", "-q")
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config(autonomy)
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        assert result.freeze_data[DELIVERED_COMMIT_KEY] == _remote_head(remote)
        assert _remote_file(remote, "output/report.md") == "final report"

    def test_uncommitted_work_is_committed_by_the_final_commit(self, delivery_repo):
        """The job-ending commit IS the last-chance versioning step: work the
        progress committer never picked up is staged, committed and pushed,
        and the seal then pins it — not the older tip."""
        ws, root, remote = delivery_repo
        stale_tip = _remote_head(remote)
        (root / "output" / "report.md").write_text("final report\n")
        (root / "output" / "environment.md").write_text("27 KB of findings\n")
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        delivered = result.freeze_data[DELIVERED_COMMIT_KEY]
        assert delivered == _remote_head(remote) != stale_tip
        assert _remote_file(remote, "output/report.md") == "final report"
        assert _remote_file(remote, "output/environment.md") == "27 KB of findings"
        assert _git(root, "status", "--porcelain") == ""

    def test_edits_inside_an_embedded_repository_are_not_a_stale_seal(
        self, delivery_repo
    ):
        """An agent's own clone in a non-ignored directory reads `` M <dir>``
        after any ``add -A`` while its working tree has edits — the outer
        commit can only record its HEAD. That must not refuse every seal."""
        ws, root, remote = delivery_repo
        nested = root / "work" / "upstream"
        nested.mkdir(parents=True)
        _git(nested, "init", "-q", "-b", "main")
        _git(nested, "config", "user.email", "agent@test.local")
        _git(nested, "config", "user.name", "Agent")
        (nested / "a.txt").write_text("1\n")
        _git(nested, "add", "-A")
        _git(nested, "commit", "-q", "-m", "upstream")
        _git(root, "add", "-A")
        _git(root, "commit", "-q", "-m", "vendor upstream")
        (nested / "a.txt").write_text("2\n")
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        assert result.freeze_data[DELIVERED_COMMIT_KEY] == _remote_head(remote)

    @pytest.mark.parametrize("autonomy", ["partial", "full"])
    def test_a_final_commit_that_cannot_land_refuses_the_seal(
        self, delivery_repo, autonomy, caplog
    ):
        """THE defect. ``git add`` fails (a stale index lock stands in for
        whatever stopped the committer), the commit's False is discarded, and
        the push then "succeeds" with "Everything up-to-date" — so nothing
        was recorded and the seal pinned the stale scaffold tip."""
        ws, root, remote = delivery_repo
        stale_tip = _remote_head(remote)
        (root / "output" / "report.md").write_text("final report\n")
        (root / ".git" / "index.lock").write_text("")
        seed_final_data()

        with caplog.at_level(logging.ERROR):
            result = finalize_job(
                make_state(), ws, MagicMock(), config=make_config(autonomy)
            )

        # The remote really is stale: this is the afa0004b shape.
        assert _remote_head(remote) == stale_tip
        assert result.freeze_data[DELIVERY_FAILED_KEY] is True
        reason = result.freeze_data[DELIVERY_ERROR_KEY]
        assert "uncommitted" in reason
        assert "output/report.md" in reason
        # No commit may be named as delivered when the tree is not in it.
        assert DELIVERED_COMMIT_KEY not in result.freeze_data
        assert any(r.levelno >= logging.ERROR for r in caplog.records)


class TestSealRefusesWhenVersioningStopped:
    """The job was configured to deliver to a repository, yet the seal cannot
    reach a working git — the observed ``head_commit: None`` shape. Both used
    to be screened out as "git versioning off / no remote", i.e. as a
    legitimate configuration, so nothing reached the freeze record."""

    def setup_method(self):
        clear_final_phase_data("test-job")

    def teardown_method(self):
        clear_final_phase_data("test-job")

    @staticmethod
    def _delivery_workspace(**git_overrides) -> MagicMock:
        ws = make_workspace(pushed=True)
        ws.config = WorkspaceManagerConfig(
            git_versioning=True, git_remote_url="http://gitea/srw/job-afa0004b.git"
        )
        ws.get_head_commit = MagicMock(return_value=None)
        for name, value in git_overrides.items():
            setattr(ws.git_manager, name, value)
        return ws

    @pytest.mark.parametrize("autonomy", ["partial", "full"])
    def test_inactive_git_on_a_delivery_workspace_refuses(self, autonomy):
        ws = self._delivery_workspace(is_active=False)
        ws.git_manager._inactive_reason = MagicMock(
            return_value="no .git on the workspace backend"
        )
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config(autonomy)
        )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True
        assert "not active" in result.freeze_data[DELIVERY_ERROR_KEY]
        assert result.freeze_data["head_commit"] is None

    def test_missing_git_manager_on_a_delivery_workspace_refuses(self):
        ws = self._delivery_workspace()
        ws.git_manager = None
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True

    def test_unreachable_origin_on_a_delivery_workspace_refuses(self):
        """Every git call failing (a wedged git channel) makes ``has_remote``
        answer False, which the push screen reads as "no remote configured"."""
        ws = self._delivery_workspace(
            has_remote=MagicMock(return_value=False),
            commit=MagicMock(return_value=False),
            get_current_commit=MagicMock(return_value=None),
        )
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True
        assert "origin" in result.freeze_data[DELIVERY_ERROR_KEY]
        ws.git_manager.push.assert_not_called()

    def test_unreadable_status_refuses(self):
        """A status that cannot be read must not read as clean."""
        ws = self._delivery_workspace(uncommitted_paths=MagicMock(return_value=None))
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert result.freeze_data[DELIVERY_FAILED_KEY] is True

    def test_a_push_that_never_ran_refuses(self):
        """The final commit landed locally, then an exception skipped the
        push: the tree is clean, and the commit exists only on the pod."""
        ws = self._delivery_workspace(
            tag=MagicMock(side_effect=RuntimeError("tag exploded")),
            has_unpushed_commits=MagicMock(return_value=True),
        )
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        ws.git_manager.push.assert_not_called()
        assert result.freeze_data[DELIVERY_FAILED_KEY] is True
        assert "not on the remote" in result.freeze_data[DELIVERY_ERROR_KEY]

    def test_a_landed_push_is_not_second_guessed_by_the_ref_probe(self):
        """``has_unpushed_commits`` answers True when the tracking ref is
        merely absent. After a push that landed that is a false alarm."""
        ws = self._delivery_workspace(has_unpushed_commits=MagicMock(return_value=True))
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data
        assert result.freeze_data[DELIVERED_COMMIT_KEY] == "abc1234"

    def test_versioning_disabled_by_configuration_is_not_a_failure(self):
        """Lite tiers force ``git_versioning`` off even when the job has a
        repository URL; there is nothing the seal could have delivered."""
        ws = self._delivery_workspace()
        ws.config = WorkspaceManagerConfig(
            git_versioning=False, git_remote_url="http://gitea/srw/job-x.git"
        )
        ws.git_manager = None
        seed_final_data()

        result = finalize_job(
            make_state(), ws, MagicMock(), config=make_config("partial")
        )

        assert DELIVERY_FAILED_KEY not in result.freeze_data
