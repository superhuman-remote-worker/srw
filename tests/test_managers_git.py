"""Unit tests for GitManager (managers package).

Tests git versioning functionality for agent workspaces.
"""

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agent.managers.git_manager import GitManager, TagInvariantViolation  # noqa: E402


@pytest.fixture
def temp_workspace():
    """Create a temporary directory for workspace testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def git_manager(temp_workspace):
    """Create a GitManager with a temporary workspace."""
    return GitManager(temp_workspace)


@pytest.fixture
def initialized_git(temp_workspace):
    """Create a GitManager with an initialized repository."""
    gm = GitManager(temp_workspace)
    gm.init_repository()
    return gm


@pytest.fixture
def bare_remote():
    """Create a local bare repository to act as a push remote."""
    with tempfile.TemporaryDirectory() as tmpdir:
        remote = Path(tmpdir) / "remote.git"
        subprocess.run(
            ["git", "init", "--bare", str(remote)],
            check=True,
            capture_output=True,
        )
        yield remote


def git_available():
    """Check if git is available on the system."""
    return shutil.which("git") is not None


# Skip all tests if git is not available
pytestmark = pytest.mark.skipif(
    not git_available(), reason="Git not available on system"
)


class TestHasUnpushedCommits:
    """Tests for has_unpushed_commits (drives per-turn push decisions)."""

    def test_false_when_no_remote(self, initialized_git):
        """No remote configured → nothing to push."""
        assert initialized_git.has_unpushed_commits() is False

    def test_true_when_remote_configured_but_never_pushed(
        self, initialized_git, bare_remote
    ):
        """Remote exists but nothing pushed yet → the initial commit is unpushed."""
        initialized_git.add_remote("origin", str(bare_remote))
        assert initialized_git.has_unpushed_commits() is True

    def test_false_after_push(self, initialized_git, bare_remote):
        """After a successful push the branch is fully synced."""
        initialized_git.add_remote("origin", str(bare_remote))
        assert initialized_git.push() is True
        assert initialized_git.has_unpushed_commits() is False

    def test_true_after_new_commit_following_push(
        self, initialized_git, temp_workspace, bare_remote
    ):
        """A commit made after the last push counts as unpushed."""
        initialized_git.add_remote("origin", str(bare_remote))
        initialized_git.push()
        (temp_workspace / "new.txt").write_text("change")
        initialized_git.commit("add new file")
        assert initialized_git.has_unpushed_commits() is True

    def test_false_when_inactive(self, git_manager):
        """Uninitialized repo → False (no crash)."""
        assert git_manager.has_unpushed_commits() is False


class TestPushSkipLogging:
    """push() must say why it declined.

    Regression guard for the defect behind job bbce4bed: both early returns
    used to be silent, and five of the six call sites discard the return
    value, so a job could fail to push across every phase boundary and leave
    no trace in any log.
    """

    def test_inactive_repo_logs_reason(self, git_manager, caplog):
        """No .git → WARNING naming the path that was checked."""
        with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
            assert git_manager.push() is False
        assert any(
            "git not active" in r.message and ".git" in r.message
            for r in caplog.records
        ), caplog.text

    def test_missing_remote_logs_reason(self, initialized_git, caplog):
        """Active repo but no remote → WARNING naming the remote."""
        with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
            assert initialized_git.push() is False
        assert any("no 'origin' remote" in r.message for r in caplog.records), (
            caplog.text
        )

    def test_missing_remote_names_the_requested_remote(self, initialized_git, caplog):
        """A non-default remote name appears verbatim, not hardcoded 'origin'."""
        with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
            assert initialized_git.push(remote="upstream") is False
        assert any("no 'upstream' remote" in r.message for r in caplog.records), (
            caplog.text
        )

    def test_successful_push_logs_no_skip_warning(
        self, initialized_git, bare_remote, caplog
    ):
        """Happy path stays quiet — the new logging must not become noise."""
        initialized_git.add_remote("origin", str(bare_remote))
        with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
            assert initialized_git.push() is True
        assert not any("push skipped" in r.message for r in caplog.records), caplog.text

    def test_git_unavailable_is_distinguished_from_missing_repo(
        self, temp_workspace, caplog
    ):
        """A missing git binary reports as such, not as a missing .git."""
        with patch("shutil.which", return_value=None):
            gm = GitManager(temp_workspace)
            with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
                assert gm.push() is False
        assert any("git binary unavailable" in r.message for r in caplog.records), (
            caplog.text
        )


class TestPushDetachedHead:
    """push() with no branch must refuse on a detached HEAD.

    The old fallback guessed "main", which pushes a STALE main — reporting
    success while delivering none of the work at HEAD. Nothing legitimately
    relied on the guess: the workspace repo is never deliberately detached
    (rewind restores via ``checkout <sha> -- .`` to keep HEAD on its branch).
    """

    def _detach(self, temp_workspace):
        subprocess.run(
            ["git", "-C", str(temp_workspace), "checkout", "--detach"],
            check=True,
            capture_output=True,
        )

    def test_detached_head_refuses_and_logs(
        self, initialized_git, temp_workspace, bare_remote, caplog
    ):
        initialized_git.add_remote("origin", str(bare_remote))
        self._detach(temp_workspace)
        with caplog.at_level(logging.WARNING, logger="agent.managers.git_manager"):
            assert initialized_git.push() is False
        assert any(
            "push refused" in r.message and "detached" in r.message
            for r in caplog.records
        ), caplog.text

    def test_detached_head_does_not_push_a_stale_branch(
        self, initialized_git, temp_workspace, bare_remote
    ):
        """The silent-wrong-ref scenario end to end: work committed on a
        detached HEAD must not become a no-op push of the old branch tip."""
        initialized_git.add_remote("origin", str(bare_remote))
        branch = initialized_git.current_branch()
        assert initialized_git.push() is True
        before = initialized_git.rev_parse(f"origin/{branch}")
        self._detach(temp_workspace)
        (temp_workspace / "detached.txt").write_text("work")
        assert initialized_git.commit("work while detached") is True
        assert initialized_git.push() is False
        assert initialized_git.rev_parse(f"origin/{branch}") == before

    def test_explicit_branch_still_pushes_while_detached(
        self, initialized_git, temp_workspace, bare_remote
    ):
        initialized_git.add_remote("origin", str(bare_remote))
        branch = initialized_git.current_branch()
        self._detach(temp_workspace)
        assert initialized_git.push(branch=branch) is True


class TestGitManagerCheckoutBranch:
    """checkout_branch backs the repo_checkout tool — the only way a
    shell-less worker can move HEAD in an attached clone."""

    def test_switches_to_an_existing_branch(self, initialized_git, temp_workspace):
        subprocess.run(
            ["git", "-C", str(temp_workspace), "branch", "feature"],
            check=True,
            capture_output=True,
        )
        assert initialized_git.checkout_branch("feature") is True
        assert initialized_git.current_branch() == "feature"

    def test_missing_branch_fails_without_create(self, initialized_git):
        before = initialized_git.current_branch()
        assert initialized_git.checkout_branch("missing") is False
        assert initialized_git.current_branch() == before

    def test_create_makes_and_switches_to_a_new_branch(self, initialized_git):
        assert initialized_git.checkout_branch("job/fix-1", create=True) is True
        assert initialized_git.current_branch() == "job/fix-1"

    def test_create_tracks_an_existing_remote_branch(
        self, initialized_git, temp_workspace, bare_remote
    ):
        initialized_git.add_remote("origin", str(bare_remote))
        base = initialized_git.current_branch()
        assert initialized_git.checkout_branch("remote-only", create=True) is True
        (temp_workspace / "r.txt").write_text("r")
        initialized_git.commit("remote work")
        assert initialized_git.push() is True
        # Drop the local branch so only origin/remote-only remains.
        assert initialized_git.checkout_branch(base) is True
        subprocess.run(
            ["git", "-C", str(temp_workspace), "branch", "-D", "remote-only"],
            check=True,
            capture_output=True,
        )
        assert initialized_git.checkout_branch("remote-only", create=True) is True
        assert initialized_git.current_branch() == "remote-only"
        assert initialized_git.rev_parse("remote-only") == initialized_git.rev_parse(
            "origin/remote-only"
        )

    def test_inactive_repo_returns_false(self, git_manager):
        assert git_manager.checkout_branch("anything") is False


class TestRevParse:
    """rev_parse resolves refs so repo_push can verify what actually moved."""

    def test_resolves_head_to_a_full_sha(self, initialized_git):
        sha = initialized_git.rev_parse("HEAD")
        assert sha is not None
        assert len(sha) == 40

    def test_unknown_ref_returns_none(self, initialized_git):
        assert initialized_git.rev_parse("origin/definitely-missing") is None

    def test_inactive_repo_returns_none(self, git_manager):
        assert git_manager.rev_parse("HEAD") is None

    def test_push_moves_the_remote_tracking_ref(self, initialized_git, bare_remote):
        """A successful push updates origin/<branch>, which is how repo_push
        tells a real update from git's exit-0 'Everything up-to-date'."""
        initialized_git.add_remote("origin", str(bare_remote))
        branch = initialized_git.current_branch()
        assert initialized_git.rev_parse(f"origin/{branch}") is None
        assert initialized_git.push() is True
        assert initialized_git.rev_parse(f"origin/{branch}") == (
            initialized_git.rev_parse("HEAD")
        )


class TestGitManagerInit:
    """Tests for GitManager initialization."""

    def test_workspace_path_set(self, temp_workspace):
        """Test that workspace path is set correctly."""
        gm = GitManager(temp_workspace)
        assert gm.workspace_path == temp_workspace

    def test_git_available_detected(self, git_manager):
        """Test that git availability is detected."""
        # We know git is available because of the pytestmark
        assert git_manager._git_available is True

    def test_not_active_before_init(self, git_manager):
        """Test that is_active is False before initialization."""
        assert git_manager.is_active is False

    def test_active_after_init(self, initialized_git):
        """Test that is_active is True after initialization."""
        assert initialized_git.is_active is True


class TestGitManagerInitRepository:
    """Tests for repository initialization."""

    def test_init_creates_git_directory(self, git_manager, temp_workspace):
        """Test that init creates .git directory."""
        result = git_manager.init_repository()
        assert result is True
        assert (temp_workspace / ".git").exists()

    def test_init_creates_gitignore(self, git_manager, temp_workspace):
        """Test that init creates .gitignore with default patterns."""
        git_manager.init_repository()

        gitignore = temp_workspace / ".gitignore"
        assert gitignore.exists()

        content = gitignore.read_text()
        for pattern in GitManager.DEFAULT_IGNORE_PATTERNS:
            assert pattern in content

    def test_init_makes_initial_commit(self, initialized_git):
        """Test that init makes an initial commit."""
        log = initialized_git.log()
        assert "Initialize workspace" in log

    def test_init_idempotent(self, git_manager, temp_workspace):
        """Test that calling init twice is safe."""
        git_manager.init_repository()
        result = git_manager.init_repository()  # Second call

        assert result is True
        # Should still work normally
        assert git_manager.is_active is True

    def test_init_configures_local_user(self, initialized_git, temp_workspace):
        """Test that init configures local git user."""
        result = subprocess.run(
            ["git", "config", "user.email"],
            cwd=temp_workspace,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "agent@workspace.local" in result.stdout


class TestGitManagerCommit:
    """Tests for commit functionality."""

    def test_commit_basic(self, initialized_git, temp_workspace):
        """Test basic commit functionality."""
        # Create a file
        (temp_workspace / "test.txt").write_text("Hello")

        result = initialized_git.commit("Test commit")
        assert result is True

        log = initialized_git.log()
        assert "Test commit" in log

    def test_commit_empty_allowed(self, initialized_git):
        """Test that empty commits are allowed by default."""
        result = initialized_git.commit("Empty commit")
        assert result is True

        log = initialized_git.log()
        assert "Empty commit" in log

    def test_commit_empty_disallowed(self, initialized_git):
        """Test that empty commits can be disallowed."""
        result = initialized_git.commit("Should not commit", allow_empty=False)
        # No changes to commit
        assert result is False

    def test_commit_stages_all_changes(self, initialized_git, temp_workspace):
        """Test that commit stages all changes."""
        # Create new file
        (temp_workspace / "new.txt").write_text("new")
        # Modify existing file (gitignore)
        (temp_workspace / ".gitignore").write_text("*.log\n*.new")
        # Create in subdirectory
        subdir = temp_workspace / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested")

        result = initialized_git.commit("Multi-change commit")
        assert result is True

        # Verify all committed
        show = initialized_git.show(stat_only=True)
        assert "new.txt" in show
        assert ".gitignore" in show
        assert "nested.txt" in show

    def test_commit_returns_false_when_inactive(self, git_manager):
        """Test that commit returns False when git is not active."""
        result = git_manager.commit("Test")
        assert result is False


class TestGitManagerLog:
    """Tests for log functionality."""

    def test_log_basic(self, initialized_git, temp_workspace):
        """Test basic log output."""
        # Make a few commits
        (temp_workspace / "file1.txt").write_text("1")
        initialized_git.commit("First commit")
        (temp_workspace / "file2.txt").write_text("2")
        initialized_git.commit("Second commit")

        log = initialized_git.log()
        assert "First commit" in log
        assert "Second commit" in log

    def test_log_max_count(self, initialized_git, temp_workspace):
        """Test log respects max_count."""
        # Make several commits
        for i in range(5):
            (temp_workspace / f"file{i}.txt").write_text(str(i))
            initialized_git.commit(f"Commit {i}")

        log = initialized_git.log(max_count=3)
        # Should only have 3 commits
        lines = [line for line in log.split("\n") if line.strip()]
        assert len(lines) <= 3

    def test_log_oneline_format(self, initialized_git, temp_workspace):
        """Test log oneline format."""
        (temp_workspace / "file.txt").write_text("test")
        initialized_git.commit("Test message")

        log = initialized_git.log(oneline=True)
        # Oneline format is compact
        assert "Test message" in log

    def test_log_full_format(self, initialized_git, temp_workspace):
        """Test log full format."""
        (temp_workspace / "file.txt").write_text("test")
        initialized_git.commit("Test message")

        log = initialized_git.log(oneline=False)
        # Full format includes author, date, etc.
        assert "Author:" in log or "commit" in log.lower()

    def test_log_inactive(self, git_manager):
        """Test log message when inactive."""
        log = git_manager.log()
        assert "not available" in log.lower()


class TestGitManagerShow:
    """Tests for show functionality."""

    def test_show_head(self, initialized_git, temp_workspace):
        """Test showing HEAD commit."""
        (temp_workspace / "test.txt").write_text("Hello")
        initialized_git.commit("Add test file")

        show = initialized_git.show()
        assert "Add test file" in show
        assert "test.txt" in show

    def test_show_specific_commit(self, initialized_git, temp_workspace):
        """Test showing specific commit."""
        (temp_workspace / "file1.txt").write_text("1")
        initialized_git.commit("First")

        # Get first commit hash
        log = initialized_git.log()
        first_hash = log.split("\n")[1].split()[0] if "\n" in log else log.split()[0]

        (temp_workspace / "file2.txt").write_text("2")
        initialized_git.commit("Second")

        show = initialized_git.show(first_hash)
        assert "First" in show or "Initialize" in show

    def test_show_stat_only(self, initialized_git, temp_workspace):
        """Test show with stat_only."""
        (temp_workspace / "test.txt").write_text("Hello" * 100)
        initialized_git.commit("Add big file")

        show = initialized_git.show(stat_only=True)
        # Stat only shows file names and stats, not full diff
        assert "test.txt" in show
        assert "insertion" in show.lower() or "+" in show

    def test_show_truncation(self, initialized_git, temp_workspace):
        """Test show truncates large output."""
        # Create a large file
        (temp_workspace / "large.txt").write_text("Line\n" * 1000)
        initialized_git.commit("Add large file")

        show = initialized_git.show(max_lines=50)
        # Should be truncated
        if "truncated" in show:
            assert "truncated" in show
        else:
            # Or just less than full output
            assert len(show.split("\n")) <= 55  # Some buffer

    def test_show_inactive(self, git_manager):
        """Test show message when inactive."""
        show = git_manager.show()
        assert "not available" in show.lower()


class TestGitManagerDiff:
    """Tests for diff functionality."""

    def test_diff_uncommitted(self, initialized_git, temp_workspace):
        """Test diff shows uncommitted changes."""
        (temp_workspace / "test.txt").write_text("Original")
        initialized_git.commit("Add file")

        (temp_workspace / "test.txt").write_text("Modified")

        diff = initialized_git.diff()
        assert "Modified" in diff or "+Modified" in diff

    def test_diff_no_changes(self, initialized_git):
        """Test diff with no changes."""
        diff = initialized_git.diff()
        assert "No differences" in diff

    def test_diff_between_refs(self, initialized_git, temp_workspace):
        """Test diff between two refs."""
        (temp_workspace / "file.txt").write_text("version1")
        initialized_git.commit("Version 1")
        initialized_git.tag("v1")

        (temp_workspace / "file.txt").write_text("version2")
        initialized_git.commit("Version 2")

        diff = initialized_git.diff("v1", "HEAD")
        assert "version" in diff.lower() or "file.txt" in diff

    def test_diff_single_ref(self, initialized_git, temp_workspace):
        """Test diff from ref to working directory."""
        (temp_workspace / "file.txt").write_text("committed")
        initialized_git.commit("Committed")

        (temp_workspace / "file.txt").write_text("uncommitted")

        diff = initialized_git.diff("HEAD")
        assert "uncommitted" in diff or "+uncommitted" in diff

    def test_diff_specific_file(self, initialized_git, temp_workspace):
        """Test diff for specific file."""
        (temp_workspace / "file1.txt").write_text("f1")
        (temp_workspace / "file2.txt").write_text("f2")
        initialized_git.commit("Add files")

        (temp_workspace / "file1.txt").write_text("f1-modified")
        (temp_workspace / "file2.txt").write_text("f2-modified")

        diff = initialized_git.diff(file_path="file1.txt")
        assert "f1" in diff
        # file2 changes should not appear in single-file diff
        assert "f2-modified" not in diff or "file1.txt" in diff

    def test_diff_truncation(self, initialized_git, temp_workspace):
        """Test diff truncates large output."""
        (temp_workspace / "large.txt").write_text("A\n" * 1000)
        initialized_git.commit("Add file")
        (temp_workspace / "large.txt").write_text("B\n" * 1000)

        diff = initialized_git.diff(max_lines=50)
        lines = diff.split("\n")
        # Should be truncated or reasonably short
        assert len(lines) <= 60 or "truncated" in diff

    def test_diff_inactive(self, git_manager):
        """Test diff message when inactive."""
        diff = git_manager.diff()
        assert "not available" in diff.lower()


class TestGitManagerStatus:
    """Tests for status functionality."""

    def test_status_clean(self, initialized_git):
        """Test status when workspace is clean."""
        status = initialized_git.status()
        assert "clean" in status.lower()

    def test_status_dirty(self, initialized_git, temp_workspace):
        """Test status with uncommitted changes."""
        (temp_workspace / "new.txt").write_text("new")

        status = initialized_git.status()
        assert "dirty" in status.lower()
        assert "new.txt" in status

    def test_status_shows_branch(self, initialized_git):
        """Test status shows current branch."""
        status = initialized_git.status()
        assert "Branch:" in status

    def test_status_categorizes_changes(self, initialized_git, temp_workspace):
        """Test status categorizes different types of changes."""
        # Untracked file
        (temp_workspace / "untracked.txt").write_text("u")

        # Modified file (modify gitignore which exists)
        (temp_workspace / ".gitignore").write_text("*.log\n*.new")

        status = initialized_git.status()
        assert "Untracked" in status or "untracked.txt" in status
        # Modified or Staged - git may show the .gitignore change in either category
        assert "Modified" in status or "Staged" in status or "gitignore" in status

    def test_status_inactive(self, git_manager):
        """Test status message when inactive."""
        status = git_manager.status()
        assert "not available" in status.lower()


class TestGitManagerHasUncommittedChanges:
    """Tests for has_uncommitted_changes functionality."""

    def test_no_changes(self, initialized_git):
        """Test returns False when no changes."""
        assert initialized_git.has_uncommitted_changes() is False

    def test_with_changes(self, initialized_git, temp_workspace):
        """Test returns True with uncommitted changes."""
        (temp_workspace / "new.txt").write_text("new")
        assert initialized_git.has_uncommitted_changes() is True

    def test_after_commit(self, initialized_git, temp_workspace):
        """Test returns False after committing changes."""
        (temp_workspace / "new.txt").write_text("new")
        initialized_git.commit("Add file")
        assert initialized_git.has_uncommitted_changes() is False

    def test_inactive(self, git_manager):
        """Test returns False when inactive."""
        assert git_manager.has_uncommitted_changes() is False


class TestGitManagerTag:
    """Tests for tag functionality."""

    def test_create_tag(self, initialized_git):
        """Test creating a simple tag."""
        result = initialized_git.tag("v1.0")
        assert result is True

        tags = initialized_git.list_tags()
        assert "v1.0" in tags

    def test_create_annotated_tag(self, initialized_git):
        """Test creating an annotated tag with message."""
        result = initialized_git.tag("v2.0", message="Release version 2.0")
        assert result is True

        tags = initialized_git.list_tags()
        assert "v2.0" in tags

    def test_tag_phase_pattern(self, initialized_git, temp_workspace):
        """Test creating phase-style tags."""
        # Simulate phase completion workflow
        (temp_workspace / "phase1.txt").write_text("phase 1 work")
        initialized_git.commit("[Phase 1 Tactical] Complete")
        initialized_git.tag("phase-1-tactical-complete")

        (temp_workspace / "phase2.txt").write_text("phase 2 work")
        initialized_git.commit("[Phase 2 Strategic] Complete")
        initialized_git.tag("phase-2-strategic-complete")

        tags = initialized_git.list_tags("phase-*")
        assert "phase-1-tactical-complete" in tags
        assert "phase-2-strategic-complete" in tags

    def test_tag_inactive(self, git_manager):
        """Test tag returns False when inactive."""
        result = git_manager.tag("test")
        assert result is False


class TestGitManagerTagIdempotency:
    """Tests for create-once tag semantics (audit-boundary tags never move)."""

    def test_retag_same_commit_is_noop(self, initialized_git, temp_workspace):
        """Re-tagging the same name at the same commit succeeds without moving."""
        (temp_workspace / "work.txt").write_text("work")
        initialized_git.commit("Phase work")
        assert initialized_git.tag("phase-tag") is True
        head = initialized_git.get_current_commit()
        assert head is not None

        assert initialized_git.tag("phase-tag") is True
        assert initialized_git.resolve_tag_commit("phase-tag") == head

    def test_retag_at_new_commit_refuses_to_move(
        self, initialized_git, temp_workspace, caplog
    ):
        """An existing tag at a different commit is never moved."""
        (temp_workspace / "one.txt").write_text("one")
        initialized_git.commit("First completion")
        assert initialized_git.tag("phase-tag", message="boundary") is True
        original = initialized_git.resolve_tag_commit("phase-tag")
        assert original is not None

        (temp_workspace / "two.txt").write_text("two")
        initialized_git.commit("Second completion")

        with caplog.at_level(logging.ERROR, logger="agent.managers.git_manager"):
            assert initialized_git.tag("phase-tag") is False

        assert "Phase tag invariant violation" in caplog.text
        assert initialized_git.resolve_tag_commit("phase-tag") == original
        assert initialized_git.get_current_commit() != original

    def test_invariant_violation_carries_shas(self):
        """TagInvariantViolation exposes tag name and both commits."""
        exc = TagInvariantViolation("t1", "aaa111", "bbb222")
        assert isinstance(exc, RuntimeError)
        assert exc.tag_name == "t1"
        assert exc.existing_sha == "aaa111"
        assert exc.head_sha == "bbb222"
        assert "aaa111" in str(exc)
        assert "bbb222" in str(exc)

    def test_resolve_tag_commit_missing_tag(self, initialized_git):
        """Resolving a nonexistent tag returns None."""
        assert initialized_git.resolve_tag_commit("no-such-tag") is None


class TestGitManagerPushRef:
    """Tests for per-ref tag delivery and the tags=False push default."""

    def test_push_ref_delivers_exactly_that_tag(
        self, initialized_git, temp_workspace, bare_remote
    ):
        """push_ref delivers the named tag and nothing else."""
        initialized_git.add_remote("origin", str(bare_remote))
        (temp_workspace / "work.txt").write_text("work")
        initialized_git.commit("Phase work")
        assert initialized_git.tag("phase-1-complete") is True
        assert initialized_git.tag("phase-2-complete") is True

        assert initialized_git.push_ref("refs/tags/phase-1-complete") is True

        result = subprocess.run(
            ["git", "tag", "-l"], cwd=bare_remote, capture_output=True, text=True
        )
        remote_tags = result.stdout.split()
        assert "phase-1-complete" in remote_tags
        assert "phase-2-complete" not in remote_tags

    def test_push_ref_without_remote(self, initialized_git):
        """push_ref returns False when no remote is configured."""
        assert initialized_git.push_ref("refs/tags/anything") is False

    def test_push_default_omits_tags(
        self, initialized_git, temp_workspace, bare_remote
    ):
        """push() no longer sprays tags; push_ref delivers them per-ref."""
        initialized_git.add_remote("origin", str(bare_remote))
        (temp_workspace / "work.txt").write_text("work")
        initialized_git.commit("Phase work")
        assert initialized_git.tag("phase-tag") is True

        assert initialized_git.push() is True
        result = subprocess.run(
            ["git", "tag", "-l"], cwd=bare_remote, capture_output=True, text=True
        )
        assert "phase-tag" not in result.stdout.split()

        assert initialized_git.push_ref("refs/tags/phase-tag") is True
        result = subprocess.run(
            ["git", "tag", "-l"], cwd=bare_remote, capture_output=True, text=True
        )
        assert "phase-tag" in result.stdout.split()

    def test_push_tags_true_still_pushes_tags(
        self, initialized_git, temp_workspace, bare_remote
    ):
        """Explicit tags=True keeps the push-all-tags behavior."""
        initialized_git.add_remote("origin", str(bare_remote))
        (temp_workspace / "work.txt").write_text("work")
        initialized_git.commit("Phase work")
        assert initialized_git.tag("explicit-tag") is True

        assert initialized_git.push(tags=True) is True
        result = subprocess.run(
            ["git", "tag", "-l"], cwd=bare_remote, capture_output=True, text=True
        )
        assert "explicit-tag" in result.stdout.split()


class TestGitManagerListTags:
    """Tests for list_tags functionality."""

    def test_list_all_tags(self, initialized_git):
        """Test listing all tags."""
        initialized_git.tag("alpha")
        initialized_git.tag("beta")
        initialized_git.tag("gamma")

        tags = initialized_git.list_tags()
        assert "alpha" in tags
        assert "beta" in tags
        assert "gamma" in tags

    def test_list_tags_with_pattern(self, initialized_git):
        """Test listing tags with pattern filter."""
        initialized_git.tag("phase-1-complete")
        initialized_git.tag("phase-2-complete")
        initialized_git.tag("release-1.0")

        phase_tags = initialized_git.list_tags("phase-*")
        assert "phase-1-complete" in phase_tags
        assert "phase-2-complete" in phase_tags
        assert "release-1.0" not in phase_tags

        release_tags = initialized_git.list_tags("release-*")
        assert "release-1.0" in release_tags
        assert "phase-1-complete" not in release_tags

    def test_list_tags_empty(self, initialized_git):
        """Test listing tags when none exist."""
        tags = initialized_git.list_tags()
        assert tags == [] or tags == [""]  # Empty list or list with empty string

    def test_list_tags_inactive(self, git_manager):
        """Test list_tags returns empty when inactive."""
        tags = git_manager.list_tags()
        assert tags == []


class TestGitManagerTruncation:
    """Tests for output truncation."""

    def test_truncate_by_lines(self, initialized_git):
        """Test truncation by line count."""
        output = "\n".join([f"Line {i}" for i in range(1000)])
        truncated = initialized_git._truncate_output(output, max_lines=100)

        # Should be truncated with message
        assert "truncated" in truncated
        assert "1000 lines" in truncated

    def test_truncate_by_words(self, initialized_git):
        """Test truncation by word count."""
        output = " ".join(["word"] * 20000)
        truncated = initialized_git._truncate_output(output, max_words=1000)

        assert "truncated" in truncated
        assert "20000 words" in truncated

    def test_no_truncation_needed(self, initialized_git):
        """Test that small output is not truncated."""
        output = "Small output\nwith few lines"
        truncated = initialized_git._truncate_output(output)

        assert truncated == output
        assert "truncated" not in truncated


class TestGitManagerGracefulDegradation:
    """Tests for graceful degradation when git is unavailable."""

    def test_init_without_git(self, temp_workspace):
        """Test initialization gracefully handles missing git."""
        with patch("shutil.which", return_value=None):
            gm = GitManager(temp_workspace)
            assert gm._git_available is False
            assert gm.is_active is False

    def test_operations_without_git(self, temp_workspace):
        """Test operations return appropriate defaults when git unavailable."""
        with patch("shutil.which", return_value=None):
            gm = GitManager(temp_workspace)

            # Init should fail gracefully
            assert gm.init_repository() is False

            # Queries should return helpful messages
            assert "not available" in gm.log().lower()
            assert "not available" in gm.show().lower()
            assert "not available" in gm.diff().lower()
            assert "not available" in gm.status().lower()

            # Mutations should return False
            assert gm.commit("test") is False
            assert gm.tag("test") is False

            # Lists should return empty
            assert gm.list_tags() == []
            assert gm.has_uncommitted_changes() is False


class TestGitManagerTimeout:
    """Tests for command timeout handling."""

    def test_timeout_default(self, initialized_git):
        """Test that default timeout is set."""
        assert GitManager.DEFAULT_TIMEOUT == 60

    def test_timeout_handling(self, initialized_git, temp_workspace):
        """Test that timeout is passed to subprocess."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                ["git", "status"], 0, "output", ""
            )

            initialized_git._run_git(["status"])

            # Verify timeout was passed
            call_kwargs = mock_run.call_args.kwargs
            assert call_kwargs["timeout"] == 60

    def test_timeout_expired_handling(self, initialized_git):
        """Test handling of timeout expiration."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(
                cmd=["git", "status"], timeout=60
            )

            result = initialized_git._run_git(["status"])

            assert result.returncode == 1
            assert "timed out" in result.stderr.lower()


class TestGitManagerExistingRepo:
    """Tests for handling existing repositories."""

    def test_init_with_existing_repo(self, temp_workspace):
        """Test initializing when repo already exists."""
        # Manually create a git repo first
        subprocess.run(["git", "init"], cwd=temp_workspace, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            cwd=temp_workspace,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            cwd=temp_workspace,
            capture_output=True,
        )
        (temp_workspace / "existing.txt").write_text("existing")
        subprocess.run(["git", "add", "-A"], cwd=temp_workspace, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Existing commit"],
            cwd=temp_workspace,
            capture_output=True,
        )

        # Now create GitManager
        gm = GitManager(temp_workspace)
        assert gm.is_active is True

        # Init should be a no-op
        result = gm.init_repository()
        assert result is True

        # Should still see existing commit
        log = gm.log()
        assert "Existing commit" in log


# ============================================================================
# Backend delegation tests (no real git needed)
# ============================================================================


def _make_mock_backend(root="/home/agent-host/workspace"):
    """Create a mock workspace backend with shell support."""
    backend = MagicMock()
    backend.supports_shell = True
    backend.root = root
    return backend


class TestGitManagerBackendInit:
    """Tests for GitManager initialization with a backend."""

    def test_use_backend_flag_set(self):
        """When backend has shell support, _use_backend is True."""
        backend = _make_mock_backend()
        gm = GitManager(Path("/tmp/ws"), backend=backend)
        assert gm._use_backend is True
        assert gm._git_available is True

    def test_use_backend_flag_not_set_without_shell(self):
        """When backend lacks shell support, _use_backend is False."""
        backend = MagicMock()
        backend.supports_shell = False
        gm = GitManager(Path("/tmp/ws"), backend=backend)
        assert gm._use_backend is False

    def test_use_backend_flag_not_set_without_backend(self, temp_workspace):
        """When no backend, _use_backend is False."""
        gm = GitManager(temp_workspace)
        assert gm._use_backend is False

    def test_remote_cwd_stored(self):
        """remote_cwd is stored for use in _run_git."""
        backend = _make_mock_backend()
        gm = GitManager(Path("/tmp/ws"), backend=backend, remote_cwd="repos/myrepo")
        assert gm._remote_cwd == "repos/myrepo"


class TestGitManagerBackendIsActive:
    """Tests for is_active with a backend."""

    def test_is_active_checks_backend_exists(self):
        """is_active should call backend.exists('.git')."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        gm = GitManager(Path("/tmp/ws"), backend=backend)
        assert gm.is_active is True
        backend.exists.assert_called_with(".git")

    def test_is_active_false_when_no_git_dir(self):
        """is_active returns False when backend says .git doesn't exist."""
        backend = _make_mock_backend()
        backend.exists.return_value = False
        gm = GitManager(Path("/tmp/ws"), backend=backend)
        assert gm.is_active is False

    def test_is_active_with_remote_cwd(self):
        """is_active checks .git relative to remote_cwd."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        gm = GitManager(Path("/tmp/ws"), backend=backend, remote_cwd="repos/myrepo")
        assert gm.is_active is True
        backend.exists.assert_called_with("repos/myrepo/.git")


class TestParseShellRunOutput:
    """Tests for _parse_shell_run_output."""

    def _make_gm(self):
        backend = _make_mock_backend()
        return GitManager(Path("/tmp/ws"), backend=backend)

    def test_parse_success_with_output(self):
        """Parse normal success output with stdout."""
        gm = self._make_gm()
        output = "Exit code: 0\n--- stdout ---\nOn branch main\nnothing to commit"
        result = gm._parse_shell_run_output(output, ["status"])
        assert result.returncode == 0
        assert "On branch main" in result.stdout
        assert "nothing to commit" in result.stdout

    def test_parse_success_no_output(self):
        """Parse success with no output."""
        gm = self._make_gm()
        output = "Exit code: 0\n(no output)"
        result = gm._parse_shell_run_output(output, ["add", "-A"])
        assert result.returncode == 0
        assert result.stdout == ""

    def test_parse_error_exit_code(self):
        """Parse non-zero exit code."""
        gm = self._make_gm()
        output = "Exit code: 128\n--- stdout ---\nfatal: not a git repository"
        result = gm._parse_shell_run_output(output, ["status"])
        assert result.returncode == 128
        assert "fatal" in result.stdout

    def test_parse_success_with_cwd_header(self):
        """The `CWD:` line f41970ae added must not leak into stdout.

        Regression guard for the defect that lost job bbce4bed's deliverable:
        the parser assumed `--- stdout ---` was the first line after
        `Exit code:`, fell through to `stdout = rest`, and handed callers the
        whole banner as command output.
        """
        gm = self._make_gm()
        output = "Exit code: 0\nCWD: /home/agent-host/workspace\n--- stdout ---\nmain"
        result = gm._parse_shell_run_output(output, ["branch", "--show-current"])
        assert result.returncode == 0
        assert result.stdout.strip() == "main"
        assert "CWD:" not in result.stdout

    def test_parse_no_output_with_cwd_header(self):
        """The empty sentinel still reads as empty once a header precedes it."""
        gm = self._make_gm()
        output = "Exit code: 0\nCWD: /home/agent-host/workspace\n(no output)"
        result = gm._parse_shell_run_output(output, ["add", "-A"])
        assert result.returncode == 0
        assert result.stdout == ""

    def test_parse_multiline_stdout_with_cwd_header(self):
        """Payload after the marker survives intact, newlines included."""
        gm = self._make_gm()
        output = (
            "Exit code: 0\nCWD: /home/agent-host/workspace\n"
            "--- stdout ---\nline one\nline two"
        )
        result = gm._parse_shell_run_output(output, ["log"])
        assert result.stdout == "line one\nline two"

    def test_failure_surfaces_output_as_stderr(self):
        """Callers log result.stderr on failure — it must not be empty.

        `git push failed: ` with nothing after the colon is what made this
        regression invisible for a day.
        """
        gm = self._make_gm()
        output = (
            "Exit code: 128\nCWD: /home/agent-host/workspace\n"
            "--- stdout ---\nfatal: invalid refspec ''"
        )
        result = gm._parse_shell_run_output(output, ["push"])
        assert result.returncode == 128
        assert "invalid refspec" in result.stderr

    def test_success_leaves_stderr_empty(self):
        """A clean run reports no error text."""
        gm = self._make_gm()
        output = "Exit code: 0\nCWD: /ws\n--- stdout ---\nfine"
        assert gm._parse_shell_run_output(output, ["status"]).stderr == ""

    def test_parse_timeout(self):
        """Parse timeout output (no Exit code prefix)."""
        gm = self._make_gm()
        output = "Command timed out after 60s"
        result = gm._parse_shell_run_output(output, ["push"])
        assert result.returncode == 1
        assert "timed out" in result.stderr

    def test_parse_interactive_prompt(self):
        """Parse interactive prompt detection."""
        gm = self._make_gm()
        output = (
            "Command appears to be waiting for input "
            "(no output change for 5s).\nUse keys mode..."
        )
        result = gm._parse_shell_run_output(output, ["commit"])
        assert result.returncode == 1
        assert "waiting for input" in result.stderr

    def test_parse_still_running(self):
        """A still-running result -> non-zero return with a clear stderr."""
        gm = self._make_gm()
        output = (
            "Exit code: -1\n--- still running ---\n"
            "Command on tab 'git' is still running: no new output for 30s and "
            "it has not completed yet (this is NOT an error ...).\n"
            "--- terminal state ---\n(partial output)"
        )
        result = gm._parse_shell_run_output(output, ["clone", "https://x"])
        assert result.returncode == -1
        assert "still running" in result.stderr.lower()
        assert result.stdout == ""

    def test_parse_malformed_exit_code(self):
        """Parse malformed Exit code line."""
        gm = self._make_gm()
        output = "Exit code: abc\n--- stdout ---\nsome output"
        result = gm._parse_shell_run_output(output, ["log"])
        assert result.returncode == 1


class TestGitManagerBackendRunGit:
    """Tests for _run_git delegation to backend."""

    def test_run_git_delegates_to_shell_run(self):
        """_run_git calls backend.shell_run with correct command."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm._run_git(["add", "-A"])

        backend.shell_run.assert_called_once_with(
            "git add -A",
            timeout=60,
            tab_name="git",
            working_dir=None,
        )
        assert result.returncode == 0

    def test_run_git_with_remote_cwd(self):
        """_run_git passes remote_cwd as working_dir."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend, remote_cwd="repos/myrepo")

        gm._run_git(["status"])

        backend.shell_run.assert_called_once_with(
            "git status",
            timeout=60,
            tab_name="git",
            working_dir="repos/myrepo",
        )

    def test_run_git_quotes_special_chars(self):
        """_run_git properly quotes arguments with special characters."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        gm._run_git(["commit", "-m", "Fix: handle 'quotes' & $pecial chars"])

        cmd = backend.shell_run.call_args[0][0]
        # shlex.quote wraps the message in single quotes
        assert "git commit -m" in cmd
        assert "Fix: handle" in cmd

    def test_run_git_custom_timeout(self):
        """_run_git passes timeout to backend.shell_run."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        gm._run_git(["push", "-u", "origin", "main"], timeout=120)

        assert backend.shell_run.call_args.kwargs["timeout"] == 120

    def test_run_git_backend_exception(self):
        """_run_git handles backend exceptions gracefully."""
        backend = _make_mock_backend()
        backend.shell_run.side_effect = ConnectionError("SSH disconnected")
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm._run_git(["status"])

        assert result.returncode == 1
        assert "SSH disconnected" in result.stderr


class TestGitManagerBackendInitRepository:
    """Tests for init_repository with a backend."""

    def test_init_checks_git_via_backend(self):
        """init_repository checks .git existence via backend."""
        backend = _make_mock_backend()
        backend.exists.return_value = True  # Already initialized
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.init_repository()

        assert result is True
        backend.exists.assert_called_with(".git")

    def test_init_writes_gitignore_via_backend(self):
        """init_repository writes .gitignore through backend."""
        backend = _make_mock_backend()
        backend.exists.return_value = False  # Not initialized yet
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        gm.init_repository()

        # Should write .gitignore through backend
        backend.write_file.assert_called_once()
        call_args = backend.write_file.call_args
        assert call_args[0][0] == ".gitignore"
        content = call_args[0][1]
        for pattern in GitManager.DEFAULT_IGNORE_PATTERNS:
            assert pattern in content


class TestGitManagerBackendClone:
    """Tests for clone() with a backend."""

    def test_clone_runs_on_backend(self):
        """clone() runs git clone via backend.shell_run."""
        backend = _make_mock_backend(root="/home/agent/workspace")
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        backend.exists.return_value = True  # .git exists after clone

        mgr = GitManager.clone(
            "https://git.example.com/repo.git",
            Path("/tmp/ws"),
            backend=backend,
        )

        assert mgr is not None
        # First call should be the clone command
        first_call = backend.shell_run.call_args_list[0]
        cmd = first_call[0][0]
        assert "git clone" in cmd
        assert "/home/agent/workspace" in cmd
        # Full single-call wait window (shell hard cap) — see clone-wait fix
        assert first_call.kwargs["timeout"] == 600

    def test_clone_with_remote_cwd(self):
        """clone() with remote_cwd clones into subdirectory."""
        backend = _make_mock_backend(root="/home/agent/workspace")
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        backend.exists.return_value = True

        mgr = GitManager.clone(
            "https://git.example.com/repo.git",
            Path("/tmp/ws/repos/myrepo"),
            backend=backend,
            remote_cwd="repos/myrepo",
        )

        assert mgr is not None
        first_call = backend.shell_run.call_args_list[0]
        cmd = first_call[0][0]
        assert "repos/myrepo" in cmd
        assert mgr._remote_cwd == "repos/myrepo"

    def test_clone_failure_returns_none(self):
        """clone() returns None when git clone fails on backend."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = (
            "Exit code: 128\n--- stdout ---\nfatal: repository not found"
        )

        mgr = GitManager.clone(
            "https://git.example.com/bad.git",
            Path("/tmp/ws"),
            backend=backend,
        )

        assert mgr is None

    def test_clone_backend_exception_returns_none(self):
        """clone() returns None when backend raises."""
        backend = _make_mock_backend()
        backend.shell_run.side_effect = ConnectionError("SSH failed")

        mgr = GitManager.clone(
            "https://git.example.com/repo.git",
            Path("/tmp/ws"),
            backend=backend,
        )

        assert mgr is None

    def test_clone_masks_credentials_in_logs(self):
        """clone() masks credentials in URL for logging."""
        backend = _make_mock_backend()
        backend.shell_run.return_value = (
            "Exit code: 128\n--- stdout ---\nfatal: auth failed"
        )

        # Should not raise or leak credentials
        mgr = GitManager.clone(
            "https://user:secret-token@git.example.com/repo.git",
            Path("/tmp/ws"),
            backend=backend,
        )
        assert mgr is None
        # The actual shell_run call DOES contain the real URL (needed for clone)
        cmd = backend.shell_run.call_args[0][0]
        assert "secret-token" in cmd  # Real URL passed to git


_STILL_RUNNING_OUTPUT = (
    "Exit code: -1\n"
    "--- still running ---\n"
    "Command on tab 'git' is still running after 600s — that is the maximum "
    "wait for one call, not an error, and it may still be producing output.\n"
    "--- terminal state ---\n"
    "Cloning into '/home/agent-host/workspace'...\n"
    "Receiving objects:  33% (9695/29378), 5.89 MiB | 1.03 MiB/s"
)

_COLLIDING_OUTPUT = (
    "Tab 'git' has a previous command still running; your new command was "
    "NOT executed.\n"
    "Wait for it to finish before sending another command here — read the tab "
    "to monitor its progress.\n"
    "--- terminal state ---\n"
    "Receiving objects:  60% (17866/29378), 22.28 MiB | 2.43 MiB/s"
)

_PROBE_OK = "Exit code: 0\n--- stdout ---\n/home/agent-host/workspace/.git"
_CONFIG_OK = "Exit code: 0\n(no output)"


class _FakeTime:
    """Deterministic stand-in for the time module: sleep() advances the clock."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.fixture
def fake_time(monkeypatch):
    import agent.managers.git_manager as git_manager_module

    fake = _FakeTime()
    monkeypatch.setattr(git_manager_module, "time", fake, raising=False)
    return fake


class TestGitManagerBackendCloneWaitsForCompletion:
    """A slow-but-healthy clone must be waited out, never abandoned.

    Repro of job 65ba6be8: the jobs-repo clone outlived the shell_run wait,
    the shell reported "still running" (explicitly not an error), and clone()
    treated it as a failure while retries collided with the busy tab.
    """

    def test_clone_waits_out_still_running_and_attaches(self, fake_time):
        """still-running result -> poll the tab, attach once the clone lands."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.side_effect = [
            _STILL_RUNNING_OUTPUT,  # initial clone call hits the wait cap
            _COLLIDING_OUTPUT,  # probe 1: tab still busy cloning
            _PROBE_OK,  # probe 2: clone finished, repo present
            _CONFIG_OK,  # git config user.email
            _CONFIG_OK,  # git config user.name
        ]

        mgr = GitManager.clone(
            "https://git.example.com/repo.git", Path("/tmp/ws"), backend=backend
        )

        assert mgr is not None
        assert backend.shell_run.call_count == 5
        clone_call = backend.shell_run.call_args_list[0]
        assert "git clone" in clone_call[0][0]
        for probe_call in backend.shell_run.call_args_list[1:3]:
            assert "rev-parse" in probe_call[0][0]
            assert probe_call.kwargs["tab_name"] == "git"

    def test_clone_tab_busy_on_entry_waits_and_attaches(self, fake_time):
        """Busy tab on entry (a retry landing mid-clone) -> wait, then attach."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.side_effect = [
            _COLLIDING_OUTPUT,  # clone command NOT executed: tab already cloning
            _PROBE_OK,  # probe: the in-flight clone finished successfully
            _CONFIG_OK,
            _CONFIG_OK,
        ]

        mgr = GitManager.clone(
            "https://git.example.com/repo.git", Path("/tmp/ws"), backend=backend
        )

        assert mgr is not None

    def test_clone_still_running_then_failed_returns_none(self, fake_time):
        """If the awaited clone dies without leaving a repo, report failure."""
        backend = _make_mock_backend()
        backend.shell_run.side_effect = [
            _STILL_RUNNING_OUTPUT,
            "Exit code: 128\n--- stdout ---\n"
            "fatal: not a git repository: '/home/agent-host/workspace/.git'",
        ]

        mgr = GitManager.clone(
            "https://git.example.com/repo.git", Path("/tmp/ws"), backend=backend
        )

        assert mgr is None
        assert backend.shell_run.call_count == 2

    def test_clone_wait_gives_up_at_deadline(self, fake_time):
        """A tab that never frees up bounds the wait at the overall deadline."""
        backend = _make_mock_backend()

        def _never_finishes(*args, **kwargs):
            if backend.shell_run.call_count == 1:
                return _STILL_RUNNING_OUTPUT
            return _COLLIDING_OUTPUT

        backend.shell_run.side_effect = _never_finishes

        mgr = GitManager.clone(
            "https://git.example.com/repo.git", Path("/tmp/ws"), backend=backend
        )

        assert mgr is None
        assert backend.shell_run.call_count > 2
        assert fake_time.now >= 1800


class TestGitManagerBackendCommit:
    """Tests for commit() via backend — the core bug scenario."""

    def _make_active_gm(self):
        """Create a GitManager with backend that reports .git exists."""
        backend = _make_mock_backend()
        backend.exists.return_value = True  # .git exists → is_active
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        return GitManager(Path("/tmp/ws"), backend=backend), backend

    def test_commit_calls_add_then_commit(self):
        """commit() runs git add -A then git commit via backend."""
        gm, backend = self._make_active_gm()

        result = gm.commit("Add research notes")

        assert result is True
        calls = backend.shell_run.call_args_list
        # First call: git add -A
        assert "git add -A" in calls[0][0][0]
        # Second call: git commit -m ... --allow-empty
        assert "git commit" in calls[1][0][0]
        assert "Add research notes" in calls[1][0][0]
        assert "--allow-empty" in calls[1][0][0]

    def test_commit_without_allow_empty(self):
        """commit(allow_empty=False) omits --allow-empty flag."""
        gm, backend = self._make_active_gm()

        gm.commit("Real changes", allow_empty=False)

        commit_call = backend.shell_run.call_args_list[1][0][0]
        assert "--allow-empty" not in commit_call

    def test_commit_returns_false_on_add_failure(self):
        """commit() returns False when git add fails."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = "Exit code: 1\n--- stdout ---\nfatal: error"

        result = gm.commit("Should fail")
        assert result is False

    def test_commit_nothing_to_commit_allow_empty_false(self):
        """commit(allow_empty=False) returns False on 'nothing to commit'."""
        gm, backend = self._make_active_gm()
        # add succeeds, commit says nothing to commit
        backend.shell_run.side_effect = [
            "Exit code: 0\n(no output)",  # git add -A
            "Exit code: 1\n--- stdout ---\nnothing to commit, working tree clean",
        ]

        result = gm.commit("No changes", allow_empty=False)
        assert result is False

    def test_commit_message_with_special_chars(self):
        """commit() properly quotes messages with quotes and special chars."""
        gm, backend = self._make_active_gm()

        gm.commit("[Phase 1] todo_1: Search for 'Rehabilitation' & $costs")

        commit_call = backend.shell_run.call_args_list[1][0][0]
        # shlex.quote should wrap the message safely
        assert "git commit -m" in commit_call

    def test_commit_message_with_newlines(self):
        """commit() handles multiline commit messages."""
        gm, backend = self._make_active_gm()

        gm.commit("Line one\nLine two\nLine three")

        commit_call = backend.shell_run.call_args_list[1][0][0]
        assert "git commit -m" in commit_call

    def test_commit_not_active_returns_false(self):
        """commit() returns False when git is not active."""
        backend = _make_mock_backend()
        backend.exists.return_value = False  # .git doesn't exist
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.commit("test")
        assert result is False
        backend.shell_run.assert_not_called()


class TestGitManagerBackendPush:
    """Tests for push() via backend."""

    def test_push_delegates_to_backend(self):
        """push() runs git push via backend shell_run."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.side_effect = [
            # has_remote → git remote get-url origin
            "Exit code: 0\n--- stdout ---\nhttps://git.example.com/repo.git",
            # auto-detect branch → git branch --show-current
            "Exit code: 0\n--- stdout ---\nmain",
            # git push -u origin main
            "Exit code: 0\n(no output)",
        ]
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.push()

        assert result is True
        push_call = backend.shell_run.call_args_list[2][0][0]
        assert "git push -u origin main" in push_call

    def test_push_with_120s_timeout(self):
        """push() uses 120s timeout for the push command."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\nhttps://example.com"
        )
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        gm.push(branch="main")

        # Find the actual push call (3rd call: after has_remote + branch detect)
        push_calls = [
            c for c in backend.shell_run.call_args_list if "git push -u" in c[0][0]
        ]
        if push_calls:
            assert push_calls[0].kwargs["timeout"] == 120

    def test_push_returns_false_on_failure(self):
        """push() returns False when git push fails."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.side_effect = [
            "Exit code: 0\n--- stdout ---\nhttps://example.com",  # has_remote
            "Exit code: 0\n--- stdout ---\nmain",  # branch
            "Exit code: 1\n--- stdout ---\nfatal: remote rejected",  # push
        ]
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.push()
        assert result is False


class TestGitManagerBackendLogStatusDiff:
    """Tests for read-only operations via backend."""

    def _make_active_gm(self):
        backend = _make_mock_backend()
        backend.exists.return_value = True
        return GitManager(Path("/tmp/ws"), backend=backend), backend

    def test_log_returns_backend_output(self):
        """log() returns parsed stdout from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\nabc1234 Initial commit\ndef5678 Add notes"
        )

        result = gm.log()

        assert "Initial commit" in result
        assert "Add notes" in result
        cmd = backend.shell_run.call_args[0][0]
        assert "git log" in cmd
        assert "--oneline" in cmd

    def test_status_returns_backend_output(self):
        """status() parses porcelain output from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.side_effect = [
            # git status --porcelain (M_ = staged modified, ?? = untracked)
            "Exit code: 0\n--- stdout ---\nM  notes/research.md\n?? output/new.md",
            # git branch --show-current
            "Exit code: 0\n--- stdout ---\nmain",
        ]

        result = gm.status()

        assert "main" in result
        assert "notes/research.md" in result
        assert "output/new.md" in result

    def test_diff_returns_backend_output(self):
        """diff() returns parsed diff from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\ndiff --git a/file.md b/file.md\n+new line"
        )

        result = gm.diff()

        assert "diff --git" in result
        assert "+new line" in result

    def test_diff_no_changes(self):
        """diff() returns 'No differences' when output is empty."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"

        result = gm.diff()
        assert "No differences" in result

    def test_show_returns_backend_output(self):
        """show() returns parsed commit details from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\n"
            "commit abc1234\nAuthor: Agent\n\n    Add research notes\n\n"
            "diff --git a/notes/research.md b/notes/research.md"
        )

        result = gm.show()

        assert "Add research notes" in result

    def test_uncommitted_paths_reads_backend_porcelain(self):
        """The backend strips the first line's leading space; a merged-in
        stderr warning stays visible rather than reading as clean."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\nCWD: /home/agent-host/workspace\n--- stdout ---\n"
            "M output/report.md\n"
            "?? output/logs/\n"
            "warning: could not open directory 'output/root-only/': "
            "Permission denied"
        )

        paths = gm.uncommitted_paths()

        assert paths == [
            "output/report.md",
            "output/logs/",
            "warning: could not open directory 'output/root-only/': Permission denied",
        ]
        cmd = backend.shell_run.call_args[0][0]
        assert "git status --porcelain --ignore-submodules=dirty" in cmd

    def test_uncommitted_paths_clean_is_empty_not_unknown(self):
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"

        assert gm.uncommitted_paths() == []

    def test_uncommitted_paths_unknown_when_status_fails(self):
        """``has_uncommitted_changes`` reads a failure as clean; this must not."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Tab 'git' has a previous command still running; your new command "
            "was NOT executed."
        )

        assert gm.uncommitted_paths() is None
        assert gm.has_uncommitted_changes() is False  # the contrast


class TestGitManagerBackendBranch:
    """Tests for branch operations via backend."""

    def _make_active_gm(self):
        backend = _make_mock_backend()
        backend.exists.return_value = True
        return GitManager(Path("/tmp/ws"), backend=backend), backend

    def test_checkout_branch_create(self):
        """checkout_branch(create=True) creates and checks out branch."""
        gm, backend = self._make_active_gm()
        backend.shell_run.side_effect = [
            "Exit code: 0\n(no output)",  # branch --list (not found locally)
            "Exit code: 0\n(no output)",  # branch -r --list (not found remotely)
            "Exit code: 0\n(no output)",  # checkout -b new-branch
        ]

        result = gm.checkout_branch("feature/research", create=True)

        assert result is True
        checkout_call = backend.shell_run.call_args_list[2][0][0]
        assert "git checkout -b" in checkout_call
        assert "feature/research" in checkout_call

    def test_current_branch(self):
        """current_branch() returns branch name from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\nsubjob/abc123/scholar"
        )

        result = gm.current_branch()

        assert result == "subjob/abc123/scholar"


class TestGitManagerBackendTag:
    """Tests for tag operations via backend."""

    def _make_active_gm(self):
        backend = _make_mock_backend()
        backend.exists.return_value = True
        return GitManager(Path("/tmp/ws"), backend=backend), backend

    def test_create_tag(self):
        """tag() creates a tag via backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = "Exit code: 0\n(no output)"

        result = gm.tag("phase-1-complete")

        assert result is True
        cmd = backend.shell_run.call_args[0][0]
        assert "git tag" in cmd
        assert "phase-1-complete" in cmd

    def test_list_tags(self):
        """list_tags() returns tags from backend."""
        gm, backend = self._make_active_gm()
        backend.shell_run.return_value = (
            "Exit code: 0\n--- stdout ---\n"
            "phase-1-complete\nphase-2-complete\njob-completed"
        )

        tags = gm.list_tags()

        assert "phase-1-complete" in tags
        assert "phase-2-complete" in tags
        assert len(tags) == 3


class TestGitManagerBackendFromWorktree:
    """Tests for from_worktree() with a backend."""

    def test_from_worktree_checks_git_via_backend(self):
        """from_worktree() checks .git via backend.exists."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.return_value = "Exit code: 0\n(no output)"

        mgr = GitManager.from_worktree(Path("/tmp/wt"), backend=backend)

        assert mgr is not None
        backend.exists.assert_any_call(".git")

    def test_from_worktree_returns_none_when_no_git(self):
        """from_worktree() returns None when backend says no .git."""
        backend = _make_mock_backend()
        backend.exists.return_value = False

        mgr = GitManager.from_worktree(Path("/tmp/wt"), backend=backend)

        assert mgr is None

    def test_from_worktree_with_remote_cwd(self):
        """from_worktree() checks .git relative to remote_cwd."""
        backend = _make_mock_backend()
        backend.exists.return_value = True
        backend.shell_run.return_value = "Exit code: 0\n(no output)"

        mgr = GitManager.from_worktree(
            Path("/tmp/wt"),
            backend=backend,
            remote_cwd="repos/myrepo",
        )

        assert mgr is not None
        backend.exists.assert_any_call("repos/myrepo/.git")
        assert mgr._remote_cwd == "repos/myrepo"


class TestGitManagerBackendInitFullFlow:
    """Test init_repository full flow through backend."""

    def test_init_full_sequence(self):
        """init_repository runs init, config, write .gitignore, add, commit."""
        backend = _make_mock_backend()
        backend.exists.return_value = False  # Not yet initialized
        backend.shell_run.return_value = "Exit code: 0\n(no output)"
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.init_repository()

        assert result is True
        cmds = [c[0][0] for c in backend.shell_run.call_args_list]
        # Verify the sequence: init, config email, config name, add, commit
        assert cmds[0] == "git init"
        assert "user.email" in cmds[1]
        assert "user.name" in cmds[2]
        assert "git add -A" in cmds[3]
        assert "git commit" in cmds[4] and "Initialize workspace" in cmds[4]
        # .gitignore written via backend
        backend.write_file.assert_called_once()

    def test_init_fails_on_git_init_error(self):
        """init_repository returns False when git init fails."""
        backend = _make_mock_backend()
        backend.exists.return_value = False
        backend.shell_run.return_value = (
            "Exit code: 128\n--- stdout ---\nfatal: cannot create"
        )
        gm = GitManager(Path("/tmp/ws"), backend=backend)

        result = gm.init_repository()
        assert result is False
