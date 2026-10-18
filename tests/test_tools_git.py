"""Unit tests for git tools package.

Tests the git tools that wrap GitManager for LLM access.
"""

import shutil
import tempfile
from pathlib import Path

import pytest

from agent.managers.git_manager import GitManager  # noqa: E402
from agent.tools.git.git_tools import create_git_tools, GIT_TOOLS_METADATA  # noqa: E402


def git_available():
    """Check if git is available on the system."""
    return shutil.which("git") is not None


# Skip all tests if git is not available
pytestmark = pytest.mark.skipif(
    not git_available(), reason="Git not available on system"
)


class MockWorkspaceManager:
    """Mock WorkspaceManager for testing."""

    def __init__(self, workspace_path: Path, git_manager: GitManager = None):
        self._workspace_path = workspace_path
        self._git_manager = git_manager
        self.is_initialized = True
        self.job_id = "test-job-123"

    @property
    def workspace_path(self) -> Path:
        return self._workspace_path

    @property
    def git_manager(self) -> GitManager:
        return self._git_manager


class MockToolContext:
    """Mock ToolContext for testing."""

    def __init__(self, workspace_manager: MockWorkspaceManager):
        self.workspace_manager = workspace_manager

    @property
    def job_id(self):
        if self.workspace_manager:
            return self.workspace_manager.job_id
        return None

    def has_workspace(self) -> bool:
        return self.workspace_manager is not None


@pytest.fixture
def temp_workspace():
    """Create a temporary directory for workspace testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def git_manager(temp_workspace):
    """Create an initialized GitManager."""
    gm = GitManager(temp_workspace)
    gm.init_repository()
    return gm


@pytest.fixture
def context(temp_workspace, git_manager):
    """Create a mock ToolContext with git support."""
    ws = MockWorkspaceManager(temp_workspace, git_manager)
    return MockToolContext(ws)


@pytest.fixture
def tools(context):
    """Create git tools from context."""
    return create_git_tools(context)


def get_tool_by_name(tools, name):
    """Helper to find a tool by name."""
    for tool in tools:
        if tool.name == name:
            return tool
    return None


class TestGitToolsMetadata:
    """Tests for git tools metadata."""

    def test_all_tools_have_metadata(self):
        """Test that all tools have metadata entries."""
        expected_tools = ["git_log", "git_show", "git_diff", "git_status", "git_tags"]
        for tool_name in expected_tools:
            assert tool_name in GIT_TOOLS_METADATA

    def test_metadata_has_required_fields(self):
        """Test that metadata has required fields."""
        required_fields = ["module", "function", "description", "category", "phases"]
        for tool_name, meta in GIT_TOOLS_METADATA.items():
            for field in required_fields:
                assert field in meta, f"{tool_name} missing {field}"

    def test_all_tools_are_both_phases(self):
        """Test that all git tools are available in both phases."""
        for tool_name, meta in GIT_TOOLS_METADATA.items():
            phases = meta.get("phases", [])
            assert "strategic" in phases, f"{tool_name} not in strategic"
            assert "tactical" in phases, f"{tool_name} not in tactical"

    def test_all_tools_are_git_category(self):
        """Test that all git tools are in the git category."""
        for tool_name, meta in GIT_TOOLS_METADATA.items():
            assert meta.get("category") == "git"


class TestCreateGitTools:
    """Tests for create_git_tools function."""

    def test_creates_all_tools(self, tools):
        """Test that all expected tools are created."""
        tool_names = {t.name for t in tools}
        expected = {
            "git_log",
            "git_show",
            "git_diff",
            "git_status",
            "git_tags",
        }
        assert tool_names == expected

    def test_raises_without_workspace(self):
        """Test that creation fails without workspace manager."""
        ctx = MockToolContext(None)
        with pytest.raises(ValueError, match="workspace_manager"):
            create_git_tools(ctx)

    def test_raises_without_git_manager(self, temp_workspace):
        """Test that creation fails without git manager."""
        ws = MockWorkspaceManager(temp_workspace, git_manager=None)
        ctx = MockToolContext(ws)
        with pytest.raises(ValueError, match="git_manager"):
            create_git_tools(ctx)


class TestGitLogTool:
    """Tests for git_log tool."""

    def test_basic_log(self, tools):
        """Test basic log output."""
        git_log = get_tool_by_name(tools, "git_log")
        result = git_log.invoke({})

        assert "Initialize workspace" in result

    def test_log_with_max_count(self, tools, context, temp_workspace):
        """Test log respects max_count."""
        # Create some commits
        gm = context.workspace_manager.git_manager
        for i in range(5):
            (temp_workspace / f"file{i}.txt").write_text(str(i))
            gm.commit(f"Commit {i}")

        git_log = get_tool_by_name(tools, "git_log")
        result = git_log.invoke({"max_count": 3})

        # Should only show 3 commits
        lines = [line for line in result.split("\n") if line.strip()]
        assert len(lines) <= 3

    def test_log_full_format(self, tools):
        """Test log with full format."""
        git_log = get_tool_by_name(tools, "git_log")
        result = git_log.invoke({"oneline": False})

        # Full format has more details
        assert "Author" in result or "commit" in result.lower()


class TestGitShowTool:
    """Tests for git_show tool."""

    def test_show_head(self, tools, context, temp_workspace):
        """Test showing HEAD commit."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "test.txt").write_text("content")
        gm.commit("Add test file")

        git_show = get_tool_by_name(tools, "git_show")
        result = git_show.invoke({})

        assert "Add test file" in result

    def test_show_stat_only(self, tools, context, temp_workspace):
        """Test show with stat_only."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "test.txt").write_text("content")
        gm.commit("Add test file")

        git_show = get_tool_by_name(tools, "git_show")
        result = git_show.invoke({"stat_only": True})

        assert "test.txt" in result

    def test_show_specific_commit(self, tools, context, temp_workspace):
        """Test showing specific commit ref."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "file1.txt").write_text("1")
        gm.commit("First")

        (temp_workspace / "file2.txt").write_text("2")
        gm.commit("Second")

        git_show = get_tool_by_name(tools, "git_show")
        result = git_show.invoke({"commit_ref": "HEAD~1"})

        assert "First" in result


class TestGitDiffTool:
    """Tests for git_diff tool."""

    def test_diff_uncommitted(self, tools, context, temp_workspace):
        """Test diff shows uncommitted changes."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "test.txt").write_text("original")
        gm.commit("Add file")

        (temp_workspace / "test.txt").write_text("modified")

        git_diff = get_tool_by_name(tools, "git_diff")
        result = git_diff.invoke({})

        assert "modified" in result or "+modified" in result

    def test_diff_no_changes(self, tools):
        """Test diff with no changes."""
        git_diff = get_tool_by_name(tools, "git_diff")
        result = git_diff.invoke({})

        assert "No differences" in result

    def test_diff_between_refs(self, tools, context, temp_workspace):
        """Test diff between refs."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "file.txt").write_text("v1")
        gm.commit("Version 1")
        gm.tag("v1")

        (temp_workspace / "file.txt").write_text("v2")
        gm.commit("Version 2")

        git_diff = get_tool_by_name(tools, "git_diff")
        result = git_diff.invoke({"ref1": "v1", "ref2": "HEAD"})

        assert "file.txt" in result

    def test_diff_specific_file(self, tools, context, temp_workspace):
        """Test diff for specific file."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "a.txt").write_text("a")
        (temp_workspace / "b.txt").write_text("b")
        gm.commit("Add files")

        (temp_workspace / "a.txt").write_text("a-modified")
        (temp_workspace / "b.txt").write_text("b-modified")

        git_diff = get_tool_by_name(tools, "git_diff")
        result = git_diff.invoke({"file_path": "a.txt"})

        assert "a" in result


class TestGitStatusTool:
    """Tests for git_status tool."""

    def test_status_clean(self, tools):
        """Test status when clean."""
        git_status = get_tool_by_name(tools, "git_status")
        result = git_status.invoke({})

        assert "clean" in result.lower()

    def test_status_dirty(self, tools, temp_workspace):
        """Test status with changes."""
        (temp_workspace / "new.txt").write_text("new")

        git_status = get_tool_by_name(tools, "git_status")
        result = git_status.invoke({})

        assert "dirty" in result.lower()
        assert "new.txt" in result


class TestGitTagsTool:
    """Tests for git_tags tool."""

    def test_tags_default_pattern(self, tools, context):
        """Test tags with default pattern (auto-scoped to current job)."""
        gm = context.workspace_manager.git_manager
        # Tags are namespaced by job short ID (test-job-123 -> test-job-)
        gm.tag("test-job-phase-1-tactical-complete")
        gm.tag("other-jo-phase-1-tactical-complete")  # Different job's tag
        gm.tag("release-1.0")

        git_tags = get_tool_by_name(tools, "git_tags")
        result = git_tags.invoke({})

        assert "test-job-phase-1-tactical-complete" in result
        assert "other-jo" not in result
        assert "release-1.0" not in result

    def test_tags_all_jobs(self, tools, context):
        """Test tags with all_jobs=True shows all tags."""
        gm = context.workspace_manager.git_manager
        gm.tag("test-job-phase-1-tactical-complete")
        gm.tag("other-jo-phase-1-tactical-complete")

        git_tags = get_tool_by_name(tools, "git_tags")
        result = git_tags.invoke({"pattern": "*phase-*", "all_jobs": True})

        assert "test-job-phase-1-tactical-complete" in result
        assert "other-jo-phase-1-tactical-complete" in result

    def test_tags_custom_pattern(self, tools, context):
        """Test tags with custom pattern."""
        gm = context.workspace_manager.git_manager
        gm.tag("test-job-phase-1-complete")
        gm.tag("release-1.0")

        git_tags = get_tool_by_name(tools, "git_tags")
        # all_jobs=True to use pattern directly without job prefix
        result = git_tags.invoke({"pattern": "release-*", "all_jobs": True})

        assert "release-1.0" in result
        assert "phase-1-complete" not in result

    def test_tags_no_match(self, tools):
        """Test tags with no matches."""
        git_tags = get_tool_by_name(tools, "git_tags")
        result = git_tags.invoke({"pattern": "nonexistent-*"})

        assert "No tags matching" in result


class TestGitToolsInactive:
    """Tests for git tools when git is inactive."""

    def test_all_tools_handle_inactive(self, temp_workspace):
        """Test all tools return appropriate message when inactive."""
        # Create a git manager that's not initialized
        gm = GitManager(temp_workspace)
        # Don't call init_repository, so is_active will be False

        ws = MockWorkspaceManager(temp_workspace, gm)
        ctx = MockToolContext(ws)
        tools = create_git_tools(ctx)

        # Tools with required params need minimal valid input
        required_args = {}
        for tool in tools:
            args = required_args.get(tool.name, {})
            result = tool.invoke(args)
            assert "not available" in result.lower(), (
                f"{tool.name} didn't handle inactive"
            )


class TestGitToolsIntegration:
    """Integration tests for git tools workflow."""

    def test_phase_workflow(self, tools, context, temp_workspace):
        """Test simulating a phase workflow with tags."""
        gm = context.workspace_manager.git_manager

        # Simulate tactical phase work
        (temp_workspace / "work1.txt").write_text("Task 1 complete")
        gm.commit("[Phase 1 Tactical] todo_1: Complete first task")

        (temp_workspace / "work2.txt").write_text("Task 2 complete")
        gm.commit("[Phase 1 Tactical] todo_2: Complete second task")

        # Tag phase completion (namespaced by job short ID)
        gm.tag("test-job-phase-1-tactical-complete", "Phase 1 tactical phase completed")

        # Use tools to verify
        git_log = get_tool_by_name(tools, "git_log")
        log_result = git_log.invoke({"max_count": 5})
        assert "todo_1" in log_result
        assert "todo_2" in log_result

        git_tags = get_tool_by_name(tools, "git_tags")
        tags_result = git_tags.invoke({})
        assert "test-job-phase-1-tactical-complete" in tags_result

        git_show = get_tool_by_name(tools, "git_show")
        show_result = git_show.invoke(
            {"commit_ref": "test-job-phase-1-tactical-complete"}
        )
        assert "todo_2" in show_result  # Last commit before tag


# ---------------------------------------------------------------------------
# Attached repository datasources (repos/<name>/) — see
# knowledge-base/knowledge/issues/cloned_repo_checkout_cannot_reach_non_default_refs.md
# ---------------------------------------------------------------------------


@pytest.fixture
def attached_repo(tmp_path_factory):
    """A second independent repo standing in for a cloned repository datasource."""
    path = tmp_path_factory.mktemp("attached")
    gm = GitManager(path)
    gm.init_repository()
    (path / "theme.md").write_text("rheinland")
    gm.commit("docs: add theme")
    return gm


@pytest.fixture
def attached_head(attached_repo):
    """Full SHA of the attached repo's HEAD — an object the job repo has never seen."""
    import subprocess

    out = subprocess.run(
        ["git", "-C", str(attached_repo.workspace_path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


@pytest.fixture
def context_with_attached(context, attached_repo):
    context.workspace_manager.source_repos = {"WidgetShop": attached_repo}
    return context


@pytest.fixture
def tools_with_attached(context_with_attached):
    return create_git_tools(context_with_attached)


class TestAttachedRepositoryReads:
    """The git read tools must be able to address an attached repository.

    Job c4849fa1 (2026-08-16) failed because git_show answered for the job's own
    repo, returning `fatal: bad object` for a commit that was sitting in
    repos/WidgetShop/ the whole time.
    """

    def test_git_show_reads_attached_repo(self, tools_with_attached, attached_head):
        git_show = get_tool_by_name(tools_with_attached, "git_show")
        result = git_show.invoke({"commit_ref": attached_head, "repo": "WidgetShop"})
        assert "bad object" not in result
        assert "add theme" in result

    def test_unresolvable_ref_points_at_the_attached_repos(
        self, tools_with_attached, attached_head
    ):
        """The whole incident in one assertion: asking the job repo for a commit
        it cannot have must name the repos that might have it."""
        git_show = get_tool_by_name(tools_with_attached, "git_show")
        result = git_show.invoke({"commit_ref": attached_head})
        assert "WidgetShop" in result
        assert 'repo="WidgetShop"' in result

    def test_unknown_repo_name_lists_the_valid_ones(self, tools_with_attached):
        git_show = get_tool_by_name(tools_with_attached, "git_show")
        result = git_show.invoke({"commit_ref": "HEAD", "repo": "nope"})
        assert "WidgetShop" in result

    def test_git_log_reads_attached_repo(self, tools_with_attached):
        git_log = get_tool_by_name(tools_with_attached, "git_log")
        result = git_log.invoke({"repo": "WidgetShop"})
        assert "add theme" in result

    def test_git_diff_reads_attached_repo(self, tools_with_attached, attached_repo):
        (attached_repo.workspace_path / "theme.md").write_text("changed")
        git_diff = get_tool_by_name(tools_with_attached, "git_diff")
        result = git_diff.invoke({"repo": "WidgetShop"})
        assert "changed" in result or "theme.md" in result

    def test_git_status_reads_attached_repo(self, tools_with_attached, attached_repo):
        (attached_repo.workspace_path / "new.txt").write_text("x")
        git_status = get_tool_by_name(tools_with_attached, "git_status")
        result = git_status.invoke({"repo": "WidgetShop"})
        assert "new.txt" in result

    def test_git_tags_does_not_job_scope_an_attached_repo(self, tools_with_attached):
        """The {job_id}-phase-* convention is meaningless in someone else's repo."""
        git_tags = get_tool_by_name(tools_with_attached, "git_tags")
        result = git_tags.invoke({"pattern": "*", "repo": "WidgetShop"})
        assert "test-job" not in result

    def test_job_repo_still_the_default(
        self, tools_with_attached, temp_workspace, context
    ):
        """Omitting `repo` must keep reading the job's own workspace repo."""
        gm = context.workspace_manager.git_manager
        (temp_workspace / "own.txt").write_text("mine")
        gm.commit("job repo commit")
        git_log = get_tool_by_name(tools_with_attached, "git_log")
        assert "job repo commit" in git_log.invoke({})
