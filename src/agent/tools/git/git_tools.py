"""Git tools for the Universal Agent.

Provides read-only git operations over every repository in the workspace:
- git_log: View commit history
- git_show: Inspect specific commits
- git_diff: Compare changes
- git_status: Current workspace state
- git_tags: List phase milestone tags

Each tool reads the job's own repository (context.workspace_manager.git_manager)
by default, and an attached repository datasource
(context.workspace_manager.source_repos[repo]) when given ``repo``.

These tools are bound only when the agent has NO shell tools — a shell can run
git against any repository directly, and granting both gives the agent two ways
to ask one question. See ToolsConfig.__post_init__ in src/core/loader.py.
"""

import logging
from typing import Any, Dict, List, Optional

from langchain_core.tools import tool

from agent.tools.context import ToolContext

from shared.tool_catalog.definitions import (
    GIT_TOOLS_METADATA as GIT_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


# Tool metadata for registry
# Phase availability: git tools are available in BOTH phases (read-only)


def create_git_tools(context: ToolContext) -> List[Any]:
    """Create git tools with injected context.

    Every tool reads the job's own workspace repository by default, and any
    attached repository datasource when given ``repo``. Resolution happens per
    call rather than once at closure-creation: binding a single GitManager up
    front is what made these tools answer confidently about the wrong
    repository (job c4849fa1, 2026-08-16).

    Args:
        context: ToolContext with workspace_manager (which has git_manager)

    Returns:
        List of LangChain tool functions

    Raises:
        ValueError: If git manager not available
    """
    # Check if git is available via workspace manager
    if not context.has_workspace():
        raise ValueError("Git tools require workspace_manager in ToolContext")

    git_mgr = context.workspace_manager.git_manager
    if git_mgr is None:
        raise ValueError("Git tools require git_manager on workspace_manager")

    # stderr fragments git emits when a ref does not resolve. Matched so that
    # "fatal: bad object 5e08d4fa" can point at the repo that actually has it.
    ref_failure_markers = (
        "bad object",
        "unknown revision",
        "bad revision",
        "ambiguous argument",
        "not a valid object name",
    )

    def _attached() -> Dict[str, Any]:
        """Cloned repository datasources, by clone-directory name."""
        return getattr(context.workspace_manager, "source_repos", None) or {}

    def _pick(repo: Optional[str]):
        """Return (git_manager, error). Exactly one of the two is None."""
        if not repo:
            return git_mgr, None
        attached = _attached()
        if repo not in attached:
            known = ", ".join(sorted(attached)) or "(none attached)"
            return None, (
                f"Unknown repository {repo!r}. Attached repositories: {known}. "
                "Omit `repo` to read this job's own workspace repository."
            )
        return attached[repo], None

    def _guide(result: str, repo: Optional[str]) -> str:
        """Point an unresolved ref at the attached repos that might hold it.

        A bare `fatal: bad object` is a true answer to a question the agent did
        not mean to ask. Job c4849fa1 read one as proof that its attached
        repository was unusable and paged its operator, while the commit sat in
        repos/WidgetShop/ the whole time.
        """
        if repo or not isinstance(result, str):
            return result
        if not any(marker in result.lower() for marker in ref_failure_markers):
            return result
        names = sorted(_attached())
        if not names:
            return result
        suggestion = " or ".join(f'repo="{name}"' for name in names)
        return (
            f"{result.rstrip()}\n\n"
            "This searched the job's OWN workspace repository. Attached "
            "repository datasources are separate checkouts under repos/ and "
            f"are not searched by default: {', '.join(names)}. "
            f"Retry with {suggestion} to read one of them."
        )

    @tool
    def git_log(
        max_count: int = 10,
        oneline: bool = True,
        repo: Optional[str] = None,
    ) -> str:
        """View commit history.

        Use this to review what was accomplished in previous phases or to
        understand the history of workspace changes.

        Args:
            max_count: Maximum number of commits to show (default: 10)
            oneline: Show compact one-line format (default: True).
                     Set to False for full commit details.
            repo: Attached repository to read (clone-directory name, e.g.
                  "WidgetShop"). Omit for this job's own workspace repo.

        Returns:
            Formatted commit history
        """
        mgr, error = _pick(repo)
        if error:
            return error
        if not mgr.is_active:
            return "Git versioning not available for this workspace"

        return _guide(mgr.log(max_count=max_count, oneline=oneline), repo)

    @tool
    def git_show(
        commit_ref: str = "HEAD",
        stat_only: bool = False,
        max_lines: int = 500,
        repo: Optional[str] = None,
    ) -> str:
        """Show details of a specific commit.

        Use this to inspect what changes were made in a particular commit,
        including the full diff or just file statistics.

        Args:
            commit_ref: Commit reference to show (default: "HEAD")
                       Examples: "HEAD", "HEAD~1", commit hash, tag name
            stat_only: If True, show only file statistics without full diff
                       (default: False). Use this for a quick overview.
            max_lines: Maximum output lines before truncation (default: 500)
            repo: Attached repository to read (clone-directory name, e.g.
                  "WidgetShop"). Omit for this job's own workspace repo.

        Returns:
            Commit details including message and diff (or stats)
        """
        mgr, error = _pick(repo)
        if error:
            return error
        if not mgr.is_active:
            return "Git versioning not available for this workspace"

        return _guide(
            mgr.show(
                commit_ref=commit_ref,
                stat_only=stat_only,
                max_lines=max_lines,
            ),
            repo,
        )

    @tool
    def git_diff(
        ref1: Optional[str] = None,
        ref2: Optional[str] = None,
        file_path: Optional[str] = None,
        max_lines: int = 500,
        repo: Optional[str] = None,
    ) -> str:
        """Show differences between commits or uncommitted changes.

        Supports multiple comparison modes:
        - No arguments: Show uncommitted changes (working directory vs HEAD)
        - One ref: Compare that ref to working directory
        - Two refs: Compare between refs (ref1..ref2)

        Args:
            ref1: First reference (optional). Examples: "HEAD~5", tag, hash
            ref2: Second reference (optional). Only used with ref1.
            file_path: Limit diff to a specific file (optional)
            max_lines: Maximum output lines before truncation (default: 500)
            repo: Attached repository to read (clone-directory name, e.g.
                  "WidgetShop"). Omit for this job's own workspace repo.

        Returns:
            Diff output showing changes
        """
        mgr, error = _pick(repo)
        if error:
            return error
        if not mgr.is_active:
            return "Git versioning not available for this workspace"

        return _guide(
            mgr.diff(
                ref1=ref1,
                ref2=ref2,
                file_path=file_path,
                max_lines=max_lines,
            ),
            repo,
        )

    @tool
    def git_status(repo: Optional[str] = None) -> str:
        """Show current workspace git status.

        Use this to see which files have been modified, added, or deleted
        since the last commit.

        Args:
            repo: Attached repository to read (clone-directory name, e.g.
                  "WidgetShop"). Omit for this job's own workspace repo.

        Returns:
            Status output showing modified/untracked files
        """
        mgr, error = _pick(repo)
        if error:
            return error
        if not mgr.is_active:
            return "Git versioning not available for this workspace"

        return mgr.status()

    @tool
    def git_tags(
        pattern: str = "phase-*",
        all_jobs: bool = False,
        repo: Optional[str] = None,
    ) -> str:
        """List git tags, filtered by pattern.

        Use this to see phase milestones and understand progression,
        especially useful after context compaction when phase history
        may not be in conversation context.

        By default, only shows tags prefixed for the current job. ``all_jobs``
        removes that prefix filter for legacy/imported repositories; new root
        jobs use isolated repositories, so it does not expose project history.

        Args:
            pattern: Glob pattern to filter tags (default: "phase-*")
                    Use "*" to list all tags.
            all_jobs: If True, show every matching tag in this repository.
                     If False (default), only show this job's prefixed tags.
            repo: Attached repository to read (clone-directory name, e.g.
                  "WidgetShop"). Omit for this job's own workspace repo.
                  The job-id prefix is never applied to an attached repo —
                  the phase-tag convention is this platform's, not theirs.

        Returns:
            Comma-separated list of matching tags, or message if none found
        """
        mgr, error = _pick(repo)
        if error:
            return error
        if not mgr.is_active:
            return "Git versioning not available for this workspace"

        # Auto-scope to current job unless all_jobs requested. Never for an
        # attached repository: "{job_id}-phase-*" means nothing in someone
        # else's history and would silently return "no tags found".
        if not repo and not all_jobs and context.job_id:
            pattern = f"{context.job_id[:8]}-{pattern}"

        tags = mgr.list_tags(pattern=pattern)

        if not tags or tags == [""]:
            return f"No tags matching pattern '{pattern}'"

        return ", ".join(tags)

    return [
        git_log,
        git_show,
        git_diff,
        git_status,
        git_tags,
    ]
