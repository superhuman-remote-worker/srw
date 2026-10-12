"""Integration tests for the subagent worktree environment (the
``isolation: worktree`` path of ``src.subagents.child``).

Uses a real git repo + FilesystemTestBackend to verify acquire_reader_env:
worktree created/removed on the (test) workspace, the tools loaded are exactly
the names given (the caller selects them — U3 WP4 removed the environment's
own filter), child writes land in the worktree (isolated from the parent
root), and the child context shares the parent's orchestrator_client + job
metadata (so citations stay under the parent job).
"""

import subprocess

import pytest

from agent.tools.context import ToolContext
from agent.tools.delegation.reader_env import (
    acquire_reader_env,
    release_reader_env,
)

from tests._fs_backend import FilesystemTestBackend


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=cwd, capture_output=True)


@pytest.fixture
def parent_context(tmp_path):
    from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig
    from agent.managers.git_manager import GitManager

    ws = tmp_path / "workspace"
    ws.mkdir()
    _git(["init"], ws)
    _git(["config", "user.email", "t@t.com"], ws)
    _git(["config", "user.name", "T"], ws)
    (ws / "README.md").write_text("# parent")
    _git(["add", "."], ws)
    _git(["commit", "-m", "init"], ws)

    parent_ws = WorkspaceManager(
        job_id="parent-job",
        config=WorkspaceManagerConfig(structure=[], base_path=str(ws)),
        backend=FilesystemTestBackend(ws),
    )
    parent_ws._initialized = True
    parent_ws._git_manager = GitManager(ws)

    return ToolContext(
        workspace_manager=parent_ws,
        orchestrator_client=object(),  # sentinel — must be shared into the reader
        _job_metadata={"job_id": "parent-job", "project_id": "proj"},
        config={"shell": {}},
    ), ws


class TestReaderToolNames:
    """The environment loads the names it is given, verbatim: the child build
    (``select_child_tool_names``) owns the allowlist / control-plane / write
    policy decisions, and nothing here second-guesses them."""

    @pytest.mark.asyncio
    async def test_loads_exactly_the_given_names(self, parent_context):
        parent_ctx, _ = parent_context
        env = await acquire_reader_env(
            parent_ctx,
            ["read_file", "write_file", "list_files", "cite_web"],
            index=0,
            total=1,
        )
        loaded = {t.name for t in env.tools}
        assert loaded == {"read_file", "write_file", "list_files", "cite_web"}
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_an_empty_list_loads_nothing(self, parent_context):
        """``src.subagents.child`` passes ``[]`` and loads its own selection on
        the re-based context afterwards."""
        parent_ctx, _ = parent_context
        env = await acquire_reader_env(parent_ctx, [], index=0, total=1)
        assert env.tools == []
        await release_reader_env(env)


class TestAcquireReaderEnv:
    @pytest.mark.asyncio
    async def test_worktree_created_and_removed(self, parent_context):
        parent_ctx, ws = parent_context
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=0, total=2)
        assert env.worktree_path == ".worktrees/sub_0"
        assert (ws / ".worktrees" / "sub_0").is_dir()  # created on the workspace
        assert env.branch == "sub/0"

        await release_reader_env(env)
        assert not (ws / ".worktrees" / "sub_0").exists()  # removed

    @pytest.mark.asyncio
    async def test_reader_writes_land_in_worktree(self, parent_context):
        parent_ctx, ws = parent_context
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=1, total=2)
        # Write through the reader's workspace manager.
        env.context.workspace_manager.write_file("notes.md", "reader output")
        assert (ws / ".worktrees" / "sub_1" / "notes.md").read_text() == "reader output"
        assert not (ws / "notes.md").exists()  # NOT in the parent root
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_reader_shares_orchestrator_and_job_metadata(self, parent_context):
        parent_ctx, _ = parent_context
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=0, total=1)
        # Same objects → citations/orchestrator calls stay under the parent job.
        assert env.context.orchestrator_client is parent_ctx.orchestrator_client
        assert env.context._job_metadata is parent_ctx._job_metadata
        assert env.context._job_metadata["job_id"] == "parent-job"
        # But a distinct, worktree-rooted workspace.
        assert env.context.workspace_manager is not parent_ctx.workspace_manager
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_reader_output_is_redacted_with_the_parents_tokens(
        self, parent_context
    ):
        # The child's fresh workspace knows no repository tokens, but its
        # shell reaches the parent's clones: the parent's known credentials
        # must carry over, or `git remote -v` in a worktree child leaks.
        from agent.core.tool_output_redaction import redact_tool_result

        token = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"  # synthetic
        parent_ctx, _ = parent_context
        parent_ctx.workspace_manager.source_repo_meta["repo"] = {"token": token}
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=0, total=1)
        assert env.context.workspace_manager.source_repo_meta == {}
        assert token in env.context.redaction_secrets
        assert token not in redact_tool_result(f"GIT_TOKEN={token}", env.context)
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_reader_isolated_from_parent_undo_and_freeze(self, parent_context):
        parent_ctx, _ = parent_context
        parent_ctx._snapshot_callback = lambda p: None
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=0, total=1)
        assert env.context._snapshot_callback is None
        assert env.context._freeze_request is None
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_port_block_mentions_range_and_worktree(self, parent_context):
        parent_ctx, _ = parent_context
        env = await acquire_reader_env(parent_ctx, ["read_file"], index=2, total=4)
        assert "8300-8399" in env.port_block  # (index+1)*100 base
        assert ".worktrees/sub_2" in env.port_block
        await release_reader_env(env)

    @pytest.mark.asyncio
    async def test_no_git_falls_back_to_scratch_subdir(self, tmp_path):
        """When the parent has no active git, a scratch subdir is used instead."""
        from agent.core.workspace import WorkspaceManager, WorkspaceManagerConfig

        ws = tmp_path / "nogit"
        ws.mkdir()
        parent_ws = WorkspaceManager(
            job_id="j",
            config=WorkspaceManagerConfig(structure=[], base_path=str(ws)),
            backend=FilesystemTestBackend(ws),
        )
        parent_ws._initialized = True  # no _git_manager set
        ctx = ToolContext(
            workspace_manager=parent_ws,
            _job_metadata={"job_id": "j"},
            config={},
        )
        env = await acquire_reader_env(ctx, ["read_file"], index=0, total=1)
        assert env.worktree_path == ".subagents/reader_0"
        assert env.branch is None
        assert (ws / ".subagents" / "reader_0").is_dir()
        env.context.workspace_manager.write_file("x.md", "y")
        assert (ws / ".subagents" / "reader_0" / "x.md").read_text() == "y"
        await release_reader_env(env)
