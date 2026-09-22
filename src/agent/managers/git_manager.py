"""Git manager for workspace versioning.

Provides a GitManager class that encapsulates all git operations for
agent workspaces. This enables automatic versioning of workspace changes,
queryable history, and phase tracking via git tags.

Usage:
    from agent.managers import GitManager

    git_mgr = GitManager(workspace_path)
    if git_mgr.is_active:
        git_mgr.commit("Complete todo_1: Extract requirements")
        git_mgr.tag("phase-1-tactical-complete")
"""

import base64
import binascii
import hashlib
import logging
import posixpath
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional
from uuid import UUID

logger = logging.getLogger(__name__)

# A jobs-repo clone can outlive a single shell_run wait (hard-capped at 600s
# in shell_manager.HARD_TIMEOUT_CAP_SECONDS); the shell then reports the
# command as "still running" — explicitly not an error. clone() keeps waiting
# on the tab up to the overall deadline and verifies the on-disk result,
# instead of abandoning a healthy transfer and colliding retries into the
# busy tab (job 65ba6be8).
_CLONE_SHELL_TIMEOUT_SECONDS = 600
_CLONE_WAIT_DEADLINE_SECONDS = 1800
_CLONE_POLL_INTERVAL_SECONDS = 10.0

# Fixed markers from shell_manager's STILL_RUNNING_*/COLLIDING_COMMAND
# templates. Neither matches the blocked-on-interactive-prompt template,
# which is a genuine failure (a credential prompt means bad auth).
_STILL_RUNNING_MARKER = "--- still running ---"
_COLLIDING_MARKER = "previous command still running"

_UNDO_REQUEST_TRAILER = "SRW-Control-Request"
_UNDO_PREPARE_TRAILER = "SRW-Undo-Prepare"
_UNDO_SOURCE_TRAILER = "SRW-Undo-Source"
_UNDO_TARGET_TRAILER = "SRW-Undo-Target"
_GIT_NUL_INTEGRITY_FRAME = "SRW-Git-NUL-Integrity"

# One `git status --porcelain` (v1) entry: one or two status columns, a space,
# the path. One column when the backend shell stripped the first line's leading
# space. Anything else (a merged-in "warning: could not open directory ...")
# is kept whole: it still means git cannot account for part of the tree.
_PORCELAIN_ENTRY = re.compile(r"^[ MADRCUT?!]{1,2} (.+)$")


@dataclass(frozen=True, slots=True)
class WorkspaceUndoCommit:
    """One idempotent workspace-undo effect recovered from Git history."""

    request_id: UUID
    commit_sha: str
    source_sha: str
    target_sha: str


@dataclass(frozen=True, slots=True)
class WorkspaceUndoPreparation:
    """Request-specific pre-restore snapshot recovered from Git history."""

    request_id: UUID
    commit_sha: str
    target_sha: str


class WorkspaceUndoInvariantViolation(RuntimeError):
    """Git contains an ambiguous or malformed marker for one undo request."""


class TagInvariantViolation(RuntimeError):
    """A phase-boundary tag already exists at a different commit.

    Phase tags are audit boundaries: once created they must never move.
    Carries the tag name plus both commits for diagnostics.
    """

    def __init__(
        self,
        tag_name: str,
        existing_sha: Optional[str],
        head_sha: Optional[str],
    ):
        self.tag_name = tag_name
        self.existing_sha = existing_sha
        self.head_sha = head_sha
        super().__init__(
            f"tag {tag_name} already at {existing_sha}, HEAD is {head_sha}"
        )


class GitManager:
    """Manages git operations for a workspace directory.

    This class provides a high-level interface for git operations within
    agent workspaces. It handles:
    - Repository initialization with .gitignore
    - Auto-commits on todo completion
    - History queries (log, show, diff)
    - Phase tracking via tags

    The manager gracefully degrades when git is not available - all methods
    return appropriate defaults rather than raising exceptions.

    Attributes:
        DEFAULT_TIMEOUT: Subprocess timeout for git commands (seconds)
    """

    # Subprocess timeout for git commands (seconds)
    DEFAULT_TIMEOUT = 60

    # Default truncation limits
    DEFAULT_MAX_LINES = 500
    DEFAULT_MAX_WORDS = 10000

    def __init__(self, workspace_path: Path, backend=None, remote_cwd=None):
        """Initialize GitManager for a workspace directory.

        Args:
            workspace_path: Path to the workspace directory
            backend: Optional WorkspaceBackend instance. When provided and
                the backend supports shell execution, git commands are
                delegated to the backend (e.g. via SSH on a remote VM)
                instead of running locally via subprocess.
            remote_cwd: Relative path within the backend root to use as the
                git working directory. Used for auxiliary repos cloned into
                subdirectories (e.g. "repos/my-repo"). When None, commands
                run in the backend's root directory.
        """
        self._workspace_path = Path(workspace_path)
        self._backend = backend
        self._remote_cwd = remote_cwd
        self._use_backend = backend is not None and getattr(
            backend, "supports_shell", False
        )

        if self._use_backend:
            # Git is available on the remote — skip local check
            self._git_available = True
        else:
            self._git_available = self._check_git_available()

        if not self._git_available:
            logger.warning("Git not available, workspace versioning disabled")

    @property
    def is_active(self) -> bool:
        """Check if git versioning is active for this workspace.

        Returns True only if git is available AND the workspace has been
        initialized as a git repository.
        """
        if not self._git_available:
            return False
        if self._use_backend:
            git_path = (
                posixpath.join(self._remote_cwd, ".git") if self._remote_cwd else ".git"
            )
            return self._backend.exists(git_path)
        return (self._workspace_path / ".git").exists()

    def _inactive_reason(self) -> str:
        """Explain why :attr:`is_active` is False, for diagnostic logging.

        Mirrors the branches in ``is_active``. Kept separate so the hot path
        stays a plain boolean and only failures pay for the string.
        """
        if not self._git_available:
            return "git binary unavailable"
        if self._use_backend:
            git_path = (
                posixpath.join(self._remote_cwd, ".git") if self._remote_cwd else ".git"
            )
            return f"no {git_path} on the workspace backend"
        return f"no .git at {self._workspace_path}"

    @property
    def workspace_path(self) -> Path:
        """Get the workspace path."""
        return self._workspace_path

    # Default .gitignore patterns for workspace repositories
    DEFAULT_IGNORE_PATTERNS = [
        "*.db",
        "*.log",
        "__pycache__/",
        ".DS_Store",
        "*.pyc",
        # Subagent spill reports (src/subagents/envelope.py) live in the parent
        # tree but are never part of the job's deliverable.
        ".subagents/",
    ]

    def init_repository(self) -> bool:
        """Initialize git repository with .gitignore.

        Creates a new git repository in the workspace directory, configures
        a local git user, creates .gitignore with sensible defaults, and
        makes an initial commit.

        If a .git directory already exists, this is a no-op and returns True.

        Returns:
            True if successful or already initialized, False on error
        """
        if not self._git_available:
            logger.warning("Cannot initialize repository: git not available")
            return False

        # Skip if already initialized
        if self._use_backend:
            git_path = (
                posixpath.join(self._remote_cwd, ".git") if self._remote_cwd else ".git"
            )
            already_init = self._backend.exists(git_path)
        else:
            already_init = (self._workspace_path / ".git").exists()

        if already_init:
            logger.info("Git repository already exists, skipping initialization")
            return True

        try:
            # Initialize repository
            result = self._run_git(["init"])
            if result.returncode != 0:
                logger.error(f"git init failed: {result.stderr}")
                return False

            # Configure local git user (avoid global config issues)
            self._run_git(["config", "user.email", "agent@workspace.local"])
            self._run_git(["config", "user.name", "Agent"])

            # Create .gitignore with sensible defaults
            gitignore_content = "\n".join(self.DEFAULT_IGNORE_PATTERNS) + "\n"
            if self._use_backend:
                gitignore_path = (
                    posixpath.join(self._remote_cwd, ".gitignore")
                    if self._remote_cwd
                    else ".gitignore"
                )
                self._backend.write_file(gitignore_path, gitignore_content)
            else:
                gitignore_path = self._workspace_path / ".gitignore"
                gitignore_path.write_text(gitignore_content)

            # Stage all files
            result = self._run_git(["add", "-A"])
            if result.returncode != 0:
                logger.warning(f"git add failed: {result.stderr}")

            # Create initial commit
            result = self._run_git(
                ["commit", "-m", "Initialize workspace", "--allow-empty"]
            )
            if result.returncode != 0:
                logger.error(f"Initial commit failed: {result.stderr}")
                return False

            logger.info(f"Initialized git repository in {self._workspace_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize git repository: {e}")
            return False

    def commit(self, message: str, allow_empty: bool = True) -> bool:
        """Stage all changes and commit.

        Stages all modified, added, and deleted files, then creates a commit
        with the provided message.

        Args:
            message: Commit message
            allow_empty: If True, allow commits with no changes (default: True)
                        Empty commits maintain the audit trail even for
                        read-only analysis tasks.

        Returns:
            True if commit succeeded, False otherwise
        """
        if not self.is_active:
            logger.debug("Git not active, skipping commit")
            return False

        try:
            # Stage all changes
            result = self._run_git(["add", "-A"])
            if result.returncode != 0:
                logger.warning(f"git add failed: {result.stderr}")
                return False

            # Create commit
            args = ["commit", "-m", message]
            if allow_empty:
                args.append("--allow-empty")

            result = self._run_git(args)

            # Check for "nothing to commit" which is OK if allow_empty=False
            if result.returncode != 0:
                if (
                    "nothing to commit" in result.stdout
                    or "nothing to commit" in result.stderr
                ):
                    if allow_empty:
                        logger.debug("No changes to commit (allow_empty=True)")
                        return True
                    else:
                        logger.debug("No changes to commit")
                        return False
                logger.warning(f"git commit failed: {result.stderr}")
                return False

            logger.debug(f"Committed: {message[:50]}...")
            return True

        except Exception as e:
            logger.error(f"Failed to commit: {e}")
            return False

    def restore_tree(self, commit_sha: str) -> bool:
        """Make worktree + index exactly match ``commit_sha``'s tree.

        Forward restore for session rewind: HEAD does not move (no
        ``reset --hard`` — that would strand the branch behind the Gitea
        remote and break the fast-forward push). ``read-tree -u --reset``
        is the one porcelain-adjacent verb that also DELETES files that are
        tracked now but absent at the target (``checkout <sha> -- .``
        leaves them behind). The caller commits the abandoned state first,
        so everything current is tracked and nothing is lost.

        Returns True on success; False on any git failure (bad SHA,
        inactive repo). Does not commit — callers follow with commit().
        """
        if not self.is_active:
            logger.debug("Git not active, skipping restore_tree")
            return False
        try:
            result = self._run_git(["read-tree", "-u", "--reset", commit_sha])
            if result.returncode != 0:
                logger.warning(f"git read-tree failed: {result.stderr}")
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to restore tree: {e}")
            return False

    @staticmethod
    def _full_git_oid(value: object) -> bool:
        """Accept full SHA-1 or SHA-256 object IDs, never abbreviations."""

        return (
            isinstance(value, str)
            and len(value) in {40, 64}
            and all(char in "0123456789abcdef" for char in value)
        )

    def _undo_marked_commits(self, marker: str) -> list[tuple[str, str]]:
        """Return ``(commit SHA, message)`` pairs matching an undo marker.

        Do not encode record boundaries with ``%x00`` here. Remote Git runs
        through the tmux-backed workspace shell, whose line-oriented capture
        drops NUL bytes. Concatenating ``%H`` and ``%B`` across that transport
        made an existing preparation look absent and let a retry append a
        second commit for the same request UUID.

        Candidate SHAs are therefore fetched one-per-line, then each message
        is read in its own command. Full object IDs make the boundary
        unambiguous without relying on any byte the transport may normalize.
        A transport/inspection failure is an invariant failure, not an empty
        result: callers must retry rather than create another idempotency
        marker on uncertain history.
        """

        try:
            result = self._run_git(
                [
                    "log",
                    "HEAD",
                    "--format=%H",
                    "--fixed-strings",
                    f"--grep={marker}",
                ]
            )
        except Exception as exc:
            raise WorkspaceUndoInvariantViolation(
                f"could not inspect Git history for undo marker {marker}"
            ) from exc
        if result.returncode != 0:
            raise WorkspaceUndoInvariantViolation(
                f"could not inspect Git history for undo marker {marker}"
            )

        commit_shas = [line.strip() for line in result.stdout.splitlines() if line]
        if any(not self._full_git_oid(commit_sha) for commit_sha in commit_shas):
            raise WorkspaceUndoInvariantViolation(
                f"Git returned a malformed commit for undo marker {marker}"
            )

        commits: list[tuple[str, str]] = []
        for commit_sha in commit_shas:
            try:
                message_result = self._run_git(
                    ["show", "--no-patch", "--format=%B", commit_sha]
                )
            except Exception as exc:
                raise WorkspaceUndoInvariantViolation(
                    f"could not inspect undo marker commit {commit_sha}"
                ) from exc
            if message_result.returncode != 0:
                raise WorkspaceUndoInvariantViolation(
                    f"could not inspect undo marker commit {commit_sha}"
                )
            commits.append((commit_sha, message_result.stdout))
        return commits

    def trees_match(self, first_ref: str, second_ref: str) -> bool:
        """Return whether two refs resolve to the exact same Git tree."""

        first = self.resolve_tree(first_ref)
        second = self.resolve_tree(second_ref)
        return first is not None and first == second

    def resolve_tree(self, ref: str) -> Optional[str]:
        """Resolve one commit-ish to its full tree object ID."""

        if not self.is_active:
            return None
        try:
            result = self._run_git(["rev-parse", f"{ref}^{{tree}}"])
            value = result.stdout.strip()
            return (
                value if result.returncode == 0 and self._full_git_oid(value) else None
            )
        except Exception:
            return None

    def is_ancestor(self, ancestor_ref: str, descendant_ref: str) -> bool:
        """Return whether ``ancestor_ref`` is reachable from ``descendant_ref``."""

        if not self.is_active:
            return False
        try:
            result = self._run_git(
                ["merge-base", "--is-ancestor", ancestor_ref, descendant_ref]
            )
            return result.returncode == 0
        except Exception:
            return False

    def workspace_matches_tree(self, ref: str) -> bool:
        """Return whether the tracked worktree/index already equals ``ref``.

        This detects the narrow crash window after ``restore_tree`` changed the
        index/worktree but before the idempotency commit was created. Ignored
        runtime files are outside Git undo by design; non-ignored untracked
        paths make the comparison fail closed.
        """

        if not self.is_active:
            return False
        try:
            worktree = self._run_git(["diff", "--quiet", ref, "--"])
            index = self._run_git(["diff", "--cached", "--quiet", ref, "--"])
            untracked = self._run_git_nul_records(
                ["ls-files", "--others", "--exclude-standard", "-z"]
            )
            return (
                worktree.returncode == 0 and index.returncode == 0 and untracked == []
            )
        except Exception:
            return False

    def commit_workspace_undo_preparation(
        self,
        *,
        request_id: UUID | str,
        target_sha: str,
    ) -> Optional[str]:
        """Snapshot the pre-undo state with a strict request/target marker."""

        try:
            canonical_request_id = UUID(str(request_id))
        except (TypeError, ValueError, AttributeError):
            return None
        if not self._full_git_oid(target_sha) or self.resolve_tree(target_sha) is None:
            return None

        message = (
            f"Prepare workspace undo to {target_sha[:12]}\n\n"
            f"{_UNDO_PREPARE_TRAILER}: {canonical_request_id}\n"
            f"{_UNDO_TARGET_TRAILER}: {target_sha}"
        )
        if not self.commit(message, allow_empty=True):
            return None
        commit_sha = self.get_current_commit()
        if not self._full_git_oid(commit_sha):
            return None
        try:
            preparation = self.find_workspace_undo_preparation(canonical_request_id)
        except WorkspaceUndoInvariantViolation:
            return None
        if preparation is None or preparation.commit_sha != commit_sha:
            return None
        return commit_sha

    def find_workspace_undo_preparation(
        self, request_id: UUID | str
    ) -> Optional[WorkspaceUndoPreparation]:
        """Recover the unique validated preparation for one request UUID."""

        if not self.is_active:
            return None
        try:
            canonical_request_id = UUID(str(request_id))
        except (TypeError, ValueError, AttributeError):
            return None

        marker = f"{_UNDO_PREPARE_TRAILER}: {canonical_request_id}"
        recovered: list[WorkspaceUndoPreparation] = []
        recovered_trees: list[str] = []
        for commit_sha, message in self._undo_marked_commits(marker):
            prepare_values = [
                line[len(f"{_UNDO_PREPARE_TRAILER}: ") :]
                for line in message.splitlines()
                if line.startswith(f"{_UNDO_PREPARE_TRAILER}: ")
            ]
            if str(canonical_request_id) not in prepare_values:
                continue
            target_values = [
                line[len(f"{_UNDO_TARGET_TRAILER}: ") :]
                for line in message.splitlines()
                if line.startswith(f"{_UNDO_TARGET_TRAILER}: ")
            ]
            if (
                prepare_values != [str(canonical_request_id)]
                or len(target_values) != 1
                or not self._full_git_oid(commit_sha)
                or not self._full_git_oid(target_values[0])
                or (commit_tree := self.resolve_tree(commit_sha)) is None
                or self.resolve_tree(target_values[0]) is None
            ):
                raise WorkspaceUndoInvariantViolation(
                    "malformed Git preparation for workspace undo "
                    f"{canonical_request_id}"
                )
            recovered.append(
                WorkspaceUndoPreparation(
                    request_id=canonical_request_id,
                    commit_sha=commit_sha,
                    target_sha=target_values[0],
                )
            )
            recovered_trees.append(commit_tree)

        if len(recovered) > 1:
            # A buggy remote lookup could append the same preparation again.
            # This is safe to converge only when every reachable marker names
            # the exact same request/target and snapshots the identical tree.
            # ``git log`` is newest-first, so retain the newest reachable
            # preparation. Any target or tree disagreement remains ambiguous.
            if (
                len({preparation.target_sha for preparation in recovered}) != 1
                or len(set(recovered_trees)) != 1
            ):
                raise WorkspaceUndoInvariantViolation(
                    "conflicting Git preparations for workspace undo "
                    f"{canonical_request_id}"
                )
        return recovered[0] if recovered else None

    def commit_workspace_undo(
        self,
        *,
        request_id: UUID | str,
        source_sha: str,
        target_sha: str,
    ) -> Optional[str]:
        """Commit an already-restored tree with its durable request marker.

        The marker and target tree become durable in one Git object creation.
        A crash can therefore leave either no marker (safe to finish/retry) or
        a complete marker whose tree is independently verified on recovery.
        """

        try:
            canonical_request_id = UUID(str(request_id))
        except (TypeError, ValueError, AttributeError):
            return None
        if not (
            self._full_git_oid(source_sha)
            and self._full_git_oid(target_sha)
            and self.workspace_matches_tree(target_sha)
        ):
            return None

        message = (
            f"Workspace undo to {target_sha[:12]}\n\n"
            f"{_UNDO_REQUEST_TRAILER}: {canonical_request_id}\n"
            f"{_UNDO_SOURCE_TRAILER}: {source_sha}\n"
            f"{_UNDO_TARGET_TRAILER}: {target_sha}"
        )
        if not self.commit(message, allow_empty=True):
            return None
        commit_sha = self.get_current_commit()
        if not self._full_git_oid(commit_sha) or not self.trees_match(
            commit_sha, target_sha
        ):
            return None
        return commit_sha

    def find_workspace_undo_commit(
        self, request_id: UUID | str
    ) -> Optional[WorkspaceUndoCommit]:
        """Recover the unique, tree-verified effect for an undo request.

        Search is restricted to ``HEAD`` history. A marker on an unrelated ref
        cannot authorize acknowledgement, while later empty/system commits are
        harmless because the marked commit remains an ancestor. Multiple valid
        effects for one request are an invariant violation and fail loudly.
        """

        if not self.is_active:
            return None
        try:
            canonical_request_id = UUID(str(request_id))
        except (TypeError, ValueError, AttributeError):
            return None

        marker = f"{_UNDO_REQUEST_TRAILER}: {canonical_request_id}"
        recovered: list[WorkspaceUndoCommit] = []
        for commit_sha, message in self._undo_marked_commits(marker):
            trailers: dict[str, list[str]] = {}
            for line in message.splitlines():
                for key in (
                    _UNDO_REQUEST_TRAILER,
                    _UNDO_SOURCE_TRAILER,
                    _UNDO_TARGET_TRAILER,
                ):
                    prefix = f"{key}: "
                    if line.startswith(prefix):
                        trailers.setdefault(key, []).append(line[len(prefix) :])

            request_values = trailers.get(_UNDO_REQUEST_TRAILER, [])
            if str(canonical_request_id) not in request_values:
                continue
            source_values = trailers.get(_UNDO_SOURCE_TRAILER, [])
            target_values = trailers.get(_UNDO_TARGET_TRAILER, [])
            if (
                request_values != [str(canonical_request_id)]
                or len(source_values) != 1
                or len(target_values) != 1
                or not self._full_git_oid(commit_sha)
                or not self._full_git_oid(source_values[0])
                or not self._full_git_oid(target_values[0])
                or not self.trees_match(commit_sha, target_values[0])
                or not self.is_ancestor(source_values[0], commit_sha)
            ):
                raise WorkspaceUndoInvariantViolation(
                    f"malformed Git marker for workspace undo {canonical_request_id}"
                )
            recovered.append(
                WorkspaceUndoCommit(
                    request_id=canonical_request_id,
                    commit_sha=commit_sha,
                    source_sha=source_values[0],
                    target_sha=target_values[0],
                )
            )

        if len(recovered) > 1:
            raise WorkspaceUndoInvariantViolation(
                f"multiple Git effects for workspace undo {canonical_request_id}"
            )
        return recovered[0] if recovered else None

    def find_latest_workspace_undo_commit(self) -> Optional[WorkspaceUndoCommit]:
        """Return the newest validated undo marker reachable from ``HEAD``.

        The marker's exact target is the logical undo cursor while HEAD still
        has that target tree. Parsing delegates to the request-specific
        recovery path so uniqueness, ancestry, and tree verification stay one
        invariant rather than two subtly different implementations.
        """

        if not self.is_active:
            return None
        for commit_sha, message in self._undo_marked_commits(
            f"{_UNDO_REQUEST_TRAILER}:"
        ):
            request_values = [
                line[len(f"{_UNDO_REQUEST_TRAILER}: ") :]
                for line in message.splitlines()
                if line.startswith(f"{_UNDO_REQUEST_TRAILER}: ")
            ]
            # A prose mention can satisfy git --grep without being a trailer.
            if not request_values:
                continue
            if len(request_values) != 1:
                raise WorkspaceUndoInvariantViolation(
                    "latest workspace undo marker has ambiguous request identity"
                )
            try:
                request_id = UUID(request_values[0])
            except (TypeError, ValueError, AttributeError) as exc:
                raise WorkspaceUndoInvariantViolation(
                    "latest workspace undo marker has invalid request identity"
                ) from exc
            effect = self.find_workspace_undo_commit(request_id)
            if effect is None or effect.commit_sha != commit_sha:
                raise WorkspaceUndoInvariantViolation(
                    f"latest workspace undo marker {request_id} is inconsistent"
                )
            return effect
        return None

    def log(self, max_count: int = 10, oneline: bool = True) -> str:
        """Get commit history.

        Args:
            max_count: Maximum number of commits to return (default: 10)
            oneline: If True, show compact one-line format (default: True)

        Returns:
            Formatted commit history, or error message if failed
        """
        if not self.is_active:
            return "Git versioning not available for this workspace"

        try:
            args = ["log", f"-{max_count}"]
            if oneline:
                args.append("--oneline")

            result = self._run_git(args)
            if result.returncode != 0:
                return f"Error: {result.stderr}"

            output = result.stdout.strip()
            return output if output else "No commits yet"

        except Exception as e:
            return f"Error getting log: {e}"

    def show(
        self,
        commit_ref: str = "HEAD",
        stat_only: bool = False,
        max_lines: int = DEFAULT_MAX_LINES,
    ) -> str:
        """Show commit details.

        Args:
            commit_ref: Commit reference (default: "HEAD")
            stat_only: If True, show only file statistics without diff
            max_lines: Maximum output lines before truncation (default: 500)

        Returns:
            Commit details and diff, or error message if failed
        """
        if not self.is_active:
            return "Git versioning not available for this workspace"

        try:
            args = ["show", commit_ref]
            if stat_only:
                args.append("--stat")

            result = self._run_git(args)
            if result.returncode != 0:
                return f"Error: {result.stderr}"

            return self._truncate_output(result.stdout, max_lines=max_lines)

        except Exception as e:
            return f"Error showing commit: {e}"

    def diff(
        self,
        ref1: Optional[str] = None,
        ref2: Optional[str] = None,
        file_path: Optional[str] = None,
        max_lines: int = DEFAULT_MAX_LINES,
    ) -> str:
        """Show differences.

        Supports multiple modes:
        - No args: Show uncommitted changes (working directory vs HEAD)
        - One ref: Compare that ref to working directory
        - Two refs: Compare between refs

        Args:
            ref1: First reference (optional)
            ref2: Second reference (optional)
            file_path: Limit diff to specific file (optional)
            max_lines: Maximum output lines before truncation (default: 500)

        Returns:
            Diff output, or error message if failed
        """
        if not self.is_active:
            return "Git versioning not available for this workspace"

        try:
            args = ["diff"]

            if ref1 and ref2:
                args.extend([ref1, ref2])
            elif ref1:
                args.append(ref1)
            # No refs = uncommitted changes

            if file_path:
                args.extend(["--", file_path])

            result = self._run_git(args)
            if result.returncode != 0:
                return f"Error: {result.stderr}"

            output = result.stdout
            if not output.strip():
                return "No differences"

            return self._truncate_output(output, max_lines=max_lines)

        except Exception as e:
            return f"Error getting diff: {e}"

    def changed_paths(self, ref1: str, ref2: str) -> list[str]:
        """Return paths whose tree entries differ between two commits.

        Session undo uses this only for its acknowledgement payload.  The
        actual restore remains :meth:`restore_tree`; failure to enumerate must
        never weaken or partially substitute for that operation.
        """

        if not self.is_active:
            return []
        try:
            paths = self._run_git_nul_records(
                ["diff", "--name-only", "-z", str(ref1), str(ref2)]
            )
            if paths is None:
                logger.warning("git diff --name-only failed")
                return []
            return paths
        except Exception as e:
            logger.warning("Failed to list changed paths: %s", e)
            return []

    def status(self) -> str:
        """Get current status with clear dirty/clean indication.

        Returns a human-readable status including:
        - Current branch
        - Whether workspace is clean or dirty
        - List of modified/staged/untracked files

        Returns:
            Formatted status string
        """
        if not self.is_active:
            return "Git versioning not available for this workspace"

        try:
            # Get porcelain status for parsing
            result = self._run_git(["status", "--porcelain"])
            if result.returncode != 0:
                return f"Error: {result.stderr}"

            # Get branch name
            branch_result = self._run_git(["branch", "--show-current"])
            branch = (
                branch_result.stdout.strip()
                if branch_result.returncode == 0
                else "unknown"
            )

            lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

            if not lines:
                return f"Branch: {branch}\nStatus: clean (no uncommitted changes)"

            # Parse status
            staged = []
            modified = []
            untracked = []

            for line in lines:
                if len(line) < 2:
                    continue
                index_status = line[0]
                worktree_status = line[1]
                filename = line[3:]

                if index_status in "MADRC":
                    staged.append(filename)
                if worktree_status in "MD":
                    modified.append(filename)
                if index_status == "?" and worktree_status == "?":
                    untracked.append(filename)

            output = [f"Branch: {branch}", "Status: dirty (uncommitted changes)"]

            if staged:
                output.append(f"\nStaged ({len(staged)}):")
                for f in staged[:10]:  # Limit to 10
                    output.append(f"  + {f}")
                if len(staged) > 10:
                    output.append(f"  ... and {len(staged) - 10} more")

            if modified:
                output.append(f"\nModified ({len(modified)}):")
                for f in modified[:10]:
                    output.append(f"  M {f}")
                if len(modified) > 10:
                    output.append(f"  ... and {len(modified) - 10} more")

            if untracked:
                output.append(f"\nUntracked ({len(untracked)}):")
                for f in untracked[:10]:
                    output.append(f"  ? {f}")
                if len(untracked) > 10:
                    output.append(f"  ... and {len(untracked) - 10} more")

            return "\n".join(output)

        except Exception as e:
            return f"Error getting status: {e}"

    def has_uncommitted_changes(self) -> bool:
        """Check if there are uncommitted changes in the workspace.

        Returns:
            True if there are uncommitted changes, False if clean or inactive
        """
        if not self.is_active:
            return False

        try:
            result = self._run_git(["status", "--porcelain"])
            return bool(result.stdout.strip())
        except Exception:
            return False

    def uncommitted_paths(self) -> Optional[list[str]]:
        """Paths whose state is not in HEAD — staged, unstaged or untracked.

        ``has_uncommitted_changes`` answers False when git cannot run, the
        right default for a progress probe and the wrong one for a seal: a
        status that cannot be read must not read as clean. This returns None
        for "unknown" (inactive repo, failed command) so the caller decides.

        Paths are best-effort labels for a message (porcelain v1, one per
        line); the emptiness of the list is the exact answer.

        ``--ignore-submodules=dirty``: an embedded repository (an agent's own
        ``git clone`` into a non-ignored directory) with edits inside it reads
        `` M <dir>`` forever — ``add -A`` can only stage its HEAD, never its
        working tree. A moved nested HEAD is still reported until committed.
        """
        if not self.is_active:
            return None
        try:
            result = self._run_git(
                ["status", "--porcelain", "--ignore-submodules=dirty"]
            )
        except Exception:
            return None
        if result.returncode != 0:
            return None
        paths = []
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            entry = _PORCELAIN_ENTRY.match(line)
            paths.append(entry.group(1).strip() if entry else line.strip())
        return paths

    def tag(self, tag_name: str, message: Optional[str] = None) -> bool:
        """Create a git tag at current HEAD (create-once, never moved).

        Phase tags are audit boundaries: if the tag already exists at the
        current HEAD commit this is a no-op; if it exists at a different
        commit the call logs a TagInvariantViolation and refuses to move it.

        Args:
            tag_name: Tag name (e.g., "phase-2-tactical-complete")
            message: Optional tag message (creates annotated tag if provided)

        Returns:
            True if the tag exists at HEAD (created or already there),
            False otherwise
        """
        if not self.is_active:
            logger.debug("Git not active, skipping tag")
            return False

        try:
            existing_sha = self.resolve_tag_commit(tag_name)
            if existing_sha:
                head_sha = self.get_current_commit()
                if existing_sha == head_sha:
                    logger.debug(f"Tag {tag_name} already at {head_sha}, no-op")
                    return True
                violation = TagInvariantViolation(tag_name, existing_sha, head_sha)
                logger.error(
                    f"Phase tag invariant violation: {violation} "
                    f"— refusing to move an audit boundary"
                )
                return False

            args = ["tag"]
            if message:
                args.extend(["-a", tag_name, "-m", message])
            else:
                args.append(tag_name)

            result = self._run_git(args)
            if result.returncode != 0:
                logger.warning(f"git tag failed: {result.stderr}")
                return False

            logger.debug(f"Created tag: {tag_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to create tag: {e}")
            return False

    def resolve_tag_commit(self, tag_name: str) -> Optional[str]:
        """Resolve a tag to the commit SHA it points at.

        Dereferences annotated tags to the tagged commit.

        Args:
            tag_name: Tag name to resolve

        Returns:
            Commit SHA, or None if the tag doesn't exist (or on error)
        """
        if not self.is_active:
            return None

        try:
            result = self._run_git(
                [
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    f"refs/tags/{tag_name}" + "^{commit}",
                ]
            )
            if result.returncode != 0:
                return None
            return result.stdout.strip() or None
        except Exception:
            return None

    def list_tags(self, pattern: Optional[str] = None) -> List[str]:
        """List git tags, optionally filtered by pattern.

        Args:
            pattern: Glob pattern (e.g., "phase-*")

        Returns:
            List of tag names, empty list if none or error
        """
        if not self.is_active:
            return []

        try:
            args = ["tag", "-l"]
            if pattern:
                args.append(pattern)

            result = self._run_git(args)
            if result.returncode != 0:
                return []

            output = result.stdout.strip()
            return output.split("\n") if output else []

        except Exception:
            return []

    # =========================================================================
    # Branch Operations
    # =========================================================================

    def checkout_branch(self, branch_name: str, create: bool = False) -> bool:
        """Checkout a branch. If create=True and branch doesn't exist, create it.

        Args:
            branch_name: Branch name to checkout
            create: If True, create the branch if it doesn't exist

        Returns:
            True if checkout succeeded, False otherwise
        """
        if not self.is_active:
            return False

        try:
            if create:
                # Check if branch exists locally
                result = self._run_git(["branch", "--list", branch_name])
                if not result.stdout.strip():
                    # Check if it exists on remote
                    result = self._run_git(
                        ["branch", "-r", "--list", f"origin/{branch_name}"]
                    )
                    if result.stdout.strip():
                        # Track remote branch
                        result = self._run_git(
                            ["checkout", "-b", branch_name, f"origin/{branch_name}"]
                        )
                        return result.returncode == 0
                    else:
                        # Create new local branch
                        result = self._run_git(["checkout", "-b", branch_name])
                        return result.returncode == 0

            # Branch exists locally, just checkout
            result = self._run_git(["checkout", branch_name])
            return result.returncode == 0

        except Exception as e:
            logger.error(f"Failed to checkout branch {branch_name}: {e}")
            return False

    def current_branch(self) -> str | None:
        """Get current branch name.

        Returns:
            Current branch name, or None if not in a git repo
        """
        if not self.is_active:
            return None

        try:
            result = self._run_git(["rev-parse", "--abbrev-ref", "HEAD"])
            return result.stdout.strip() if result.returncode == 0 else None
        except Exception:
            return None

    def get_current_commit(self) -> str | None:
        """Get the current HEAD commit SHA.

        Used as a progress-detection heuristic (e.g. the critic verdict
        tools): callers must treat failure as "unknown", not an error.

        Returns:
            Full commit SHA, or None if not in a git repo or on any error.
        """
        if not self.is_active:
            return None

        try:
            result = self._run_git(["rev-parse", "HEAD"])
            return result.stdout.strip() if result.returncode == 0 else None
        except Exception:
            return None

    def rev_parse(self, ref: str) -> str | None:
        """Resolve a ref (e.g. "HEAD", "origin/main") to a commit SHA.

        Returns:
            Full commit SHA, or None if the ref does not resolve.
        """
        if not self.is_active:
            return None

        try:
            result = self._run_git(["rev-parse", ref])
            return result.stdout.strip() if result.returncode == 0 else None
        except Exception:
            return None

    # Paths that are not part of what a round PRODUCED. Each changes on every
    # round regardless of the agent's output, so counting them makes the
    # content hash differ between two byte-identical rounds — reintroducing
    # exactly the inertness ``get_content_tree`` exists to remove.
    #
    # Entries ending in "/" match by PREFIX, everything else exactly.
    #
    # That rule is NOT the same as "not delivered to the user", and the
    # difference matters for the last entry:
    #
    # - output/job_{completion,frozen}.json carry a fresh ``timestamp``.
    # - archive/ holds ``todos_phase_{N}_{type}_{TIMESTAMP}.md``, written by
    #   ``TodoManager.archive`` (src/managers/todo.py) which ``finalize_job``
    #   calls AFTER this hash is captured — so round N's archive is a NEW
    #   tracked path staged by round N+1's ``git add -A``. That alone made the
    #   guard inert for any job whose agent used todos, i.e. essentially all
    #   of them, and it is invisible to any test that mocks the TodoManager.
    #   (It is also in ``SYNC_IGNORE_PATTERNS``, i.e. never delivered — but
    #   that is a coincidence, not the reason. See below.)
    # - feedback.md is written into the workspace ROOT by
    #   ``restore_from_feedback`` (src/graph.py) on every feedback resume —
    #   which is exactly what a RETURNED verification round triggers, via
    #   ``_internal_resume_job``. It lands between round N's capture and round
    #   N+1's, so it is a new tracked path in N+1 and the guard could never
    #   fire on the first repeated return.
    #
    #   Note it IS delivered to the user (deliberately absent from
    #   ``SYNC_IGNORE_PATTERNS``). It is excluded here because it is the
    #   round's INPUT — the critic's feedback, handed TO the agent — not
    #   anything the agent produced in response. Do not generalise this list
    #   as "the undelivered files"; that reading would be wrong here and would
    #   licence excluding real deliverables.
    #
    # Every entry was found empirically, by diffing `ls-tree` round-over-round
    # against a real workspace driven through a full return cycle. Do not add
    # to it by guess: each entry costs detection sensitivity, and a wrong one
    # blinds the guard to real work.
    NON_DELIVERABLE_PATHS = (
        "output/job_completion.json",
        "output/job_frozen.json",
        "archive/",
        "feedback.md",
    )

    def get_content_tree(self, exclude: Optional[Iterable[str]] = None) -> str | None:
        """Stable hash of the committed workspace CONTENT at HEAD.

        This is the progress signal the verification gate compares between
        rounds ("did the deliverable actually change?"). Unlike a commit SHA
        it is content-addressed, which is what makes it usable:

        - **Invariant under empty commits.** Both freeze branches of
          ``finalize_job`` and every phase boundary call
          ``commit(..., allow_empty=True)`` unconditionally, so HEAD moves on
          every round no matter what. A guard keyed on the commit SHA can
          therefore never fire whenever workspace git versioning is on — the
          default.
        - **Invariant under a re-clone.** When a round's ``push()`` fails and
          the pod is recycled, the workspace re-clones from the remote and
          HEAD reverts to an older commit. A commit comparison reads that
          infrastructure hiccup as "no progress" and escalates BACKWARDS; the
          content hash is unchanged as long as the content is.

        Derived from ``git ls-tree -r HEAD`` (mode/type/blob-sha/path for
        every tracked file) rather than ``rev-parse HEAD^{tree}`` so that the
        agent's own run bookkeeping can be excluded — see
        ``NON_DELIVERABLE_PATHS``.

        Args:
            exclude: Repo-relative paths to leave out of the hash. An entry
                ending in ``/`` matches by prefix, everything else exactly.
                Defaults to ``NON_DELIVERABLE_PATHS``; pass ``()`` to hash
                everything tracked.

        Returns:
            Hex digest, or None if not in a git repo or on any error. Like
            :meth:`get_current_commit`, callers must treat failure as
            "unknown", not as an error — it must never raise.
        """
        if not self.is_active:
            return None

        try:
            result = self._run_git(["ls-tree", "-r", "HEAD"])
            if result.returncode != 0:
                return None
            listing = result.stdout or ""
        except Exception:
            return None

        patterns = self.NON_DELIVERABLE_PATHS if exclude is None else tuple(exclude)
        exact = {p for p in patterns if not p.endswith("/")}
        prefixes = tuple(p for p in patterns if p.endswith("/"))

        kept: List[str] = []
        for line in listing.splitlines():
            # "<mode> <type> <sha>\t<path>"
            path = line.split("\t", 1)[1] if "\t" in line else ""
            if path in exact or path.startswith(prefixes):
                continue
            kept.append(line)

        # ls-tree -r already emits a stable order; sorting pins it so the
        # digest can never depend on git's output ordering.
        kept.sort()
        return hashlib.sha256("\n".join(kept).encode("utf-8")).hexdigest()

    # =========================================================================
    # Remote Operations
    # =========================================================================

    def has_remote(self, name: str = "origin") -> bool:
        """Check if a named remote is configured.

        Args:
            name: Remote name (default: "origin")

        Returns:
            True if remote exists, False otherwise
        """
        if not self.is_active:
            return False

        try:
            result = self._run_git(["remote", "get-url", name])
            return result.returncode == 0
        except Exception:
            return False

    def remote_url(self, name: str = "origin") -> Optional[str]:
        """Return the URL of a named remote, or None when unset/unreadable.

        Args:
            name: Remote name (default: "origin")
        """
        if not self.is_active:
            return None
        try:
            result = self._run_git(["remote", "get-url", name])
        except Exception:
            return None
        if result.returncode != 0:
            return None
        return result.stdout.strip() or None

    def add_remote(self, name: str, url: str) -> bool:
        """Add or update a git remote.

        If the remote already exists, updates its URL instead.

        Args:
            name: Remote name (e.g. "origin")
            url: Remote URL (may contain credentials)

        Returns:
            True if successful, False otherwise
        """
        if not self.is_active:
            return False

        masked = self._mask_url(url)

        try:
            if self.has_remote(name):
                result = self._run_git(["remote", "set-url", name, url])
                if result.returncode == 0:
                    logger.debug(f"Updated remote '{name}' -> {masked}")
                    return True
            else:
                result = self._run_git(["remote", "add", name, url])
                if result.returncode == 0:
                    logger.debug(f"Added remote '{name}' -> {masked}")
                    return True

            logger.warning(f"Failed to configure remote '{name}': {result.stderr}")
            return False

        except Exception as e:
            logger.error(f"Failed to configure remote '{name}': {e}")
            return False

    def push(
        self, remote: str = "origin", branch: Optional[str] = None, tags: bool = False
    ) -> bool:
        """Push commits and optionally tags to remote.

        Gracefully returns False if git is inactive, no remote is configured,
        or no branch was given while HEAD is detached. All of those are
        *logged at WARNING* — the first two used to return silently, which is
        how job bbce4bed lost a deliverable: every phase-boundary push
        returned False across 8 phases and left no trace anywhere, because five
        of the six call sites also discard the return value. See
        knowledge-base/knowledge/issues/deliverable_lost_to_nested_repo_commit_and_stranded_mode_a_job.md.

        Tag delivery is normally per-ref via push_ref() so one rejected
        historical tag can't spoil every subsequent push; tags=True remains
        available for explicitly pushing all tags.

        Args:
            remote: Remote name (default: "origin")
            branch: Branch to push (auto-detected if None; a detached HEAD
                cannot be auto-detected and is refused)
            tags: Also push all tags (default: False)

        Returns:
            True if push succeeded, False otherwise
        """
        if not self.is_active:
            logger.warning(f"push skipped — git not active: {self._inactive_reason()}")
            return False
        if not self.has_remote(remote):
            logger.warning(
                f"push skipped — no '{remote}' remote configured "
                f"(at {self._remote_cwd or self._workspace_path})"
            )
            return False

        try:
            # Auto-detect branch. Refuse on an EMPTY result too, not just a
            # non-zero exit: a detached HEAD reports success with no output.
            # This used to fall back to "main", which pushes a STALE main —
            # reporting success while delivering none of the work at HEAD.
            # No caller relies on the guess: the workspace repo is never
            # deliberately detached (rewind restores via `checkout <sha> -- .`
            # precisely to keep HEAD on its branch).
            if branch is None:
                result = self._run_git(["branch", "--show-current"])
                detected = result.stdout.strip() if result.returncode == 0 else ""
                if not detected:
                    logger.warning(
                        "push refused — HEAD is detached (or the current "
                        "branch could not be read) at "
                        f"{self._remote_cwd or self._workspace_path} and no "
                        "branch was given; refusing to guess 'main'"
                    )
                    return False
                branch = detected

            # Push branch
            result = self._run_git(
                ["push", "-u", remote, branch],
                timeout=120,
            )
            if result.returncode != 0:
                logger.warning(f"git push failed: {result.stderr}")
                return False

            # Push tags
            if tags:
                tag_result = self._run_git(
                    ["push", remote, "--tags"],
                    timeout=120,
                )
                if tag_result.returncode != 0:
                    logger.warning(f"git push --tags failed: {tag_result.stderr}")
                    # Don't fail overall push if only tags failed

            logger.debug(f"Pushed to {remote}/{branch}")
            return True

        except Exception as e:
            logger.warning(f"Push failed: {e}")
            return False

    def push_ref(self, ref: str, remote: str = "origin") -> bool:
        """Push a single exact ref (e.g. "refs/tags/<name>") to the remote.

        Delivers phase-boundary tags one at a time instead of spraying every
        historical tag via ``git push --tags``.

        Args:
            ref: Fully qualified ref to push (e.g. "refs/tags/<name>")
            remote: Remote name (default: "origin")

        Returns:
            True if the push succeeded, False otherwise
        """
        if not self.is_active:
            logger.warning(
                f"push_ref skipped — git not active: {self._inactive_reason()}"
            )
            return False
        if not self.has_remote(remote):
            logger.warning(
                f"push_ref skipped — no '{remote}' remote configured "
                f"(at {self._remote_cwd or self._workspace_path})"
            )
            return False

        try:
            result = self._run_git(["push", remote, f"{ref}:{ref}"], timeout=120)
            if result.returncode != 0:
                logger.warning(f"git push {ref} failed: {result.stderr}")
                return False

            logger.debug(f"Pushed {ref} to {remote}")
            return True

        except Exception as e:
            logger.warning(f"Push of {ref} failed: {e}")
            return False

    def has_unpushed_commits(
        self, remote: str = "origin", branch: Optional[str] = None
    ) -> bool:
        """Check whether the local branch has commits not yet on the remote.

        Compares HEAD against the local remote-tracking ref (e.g.
        ``origin/main``), so it does NOT perform a network round-trip — it only
        reads refs already on disk. Used to decide whether an end-of-turn push
        is worthwhile.

        Returns:
            True if at least one local commit is not on the remote. False if
            git is inactive, no remote is configured (nothing to push to), or
            the branch is already fully pushed.
        """
        if not self.is_active or not self.has_remote(remote):
            return False

        try:
            if branch is None:
                result = self._run_git(["branch", "--show-current"])
                branch = (
                    result.stdout.strip()
                    if result.returncode == 0 and result.stdout.strip()
                    else "main"
                )

            # Count commits reachable from HEAD but not from the remote-tracking
            # ref. Reads the local ref only — no fetch required.
            result = self._run_git(["rev-list", "--count", f"{remote}/{branch}..HEAD"])
            if result.returncode != 0:
                # The remote-tracking ref doesn't exist yet (nothing has ever
                # been pushed). Any commit on HEAD is therefore unpushed.
                head = self._run_git(["rev-parse", "--verify", "HEAD"])
                return head.returncode == 0
            return int((result.stdout or "").strip() or "0") > 0
        except Exception as e:
            # Be conservative: if we can't tell, assume there may be unpushed
            # work so the caller attempts a push rather than silently stranding
            # commits on the workspace pod.
            logger.debug(f"has_unpushed_commits check failed: {e}")
            return True

    def pull(self, remote: str = "origin", branch: Optional[str] = None) -> bool:
        """Pull from remote (fast-forward only).

        Gracefully returns False if no remote is configured.

        Args:
            remote: Remote name (default: "origin")
            branch: Branch to pull (auto-detected if None)

        Returns:
            True if pull succeeded, False otherwise
        """
        if not self.is_active or not self.has_remote(remote):
            return False

        try:
            if branch is None:
                result = self._run_git(["branch", "--show-current"])
                branch = result.stdout.strip() if result.returncode == 0 else "main"

            result = self._run_git(
                ["pull", remote, branch, "--ff-only"],
                timeout=120,
            )
            if result.returncode != 0:
                logger.warning(f"git pull failed: {result.stderr}")
                return False

            logger.debug(f"Pulled from {remote}/{branch}")
            return True

        except Exception as e:
            logger.warning(f"Pull failed: {e}")
            return False

    @classmethod
    def clone(
        cls,
        url: str,
        target_path: Path,
        backend=None,
        remote_cwd=None,
    ) -> Optional["GitManager"]:
        """Clone a repository to a target path.

        Args:
            url: Repository URL (may contain credentials)
            target_path: Local path to clone into (used for the GitManager
                instance; ignored for the actual clone when backend is active)
            backend: Optional WorkspaceBackend for remote execution
            remote_cwd: Relative path within backend root for the clone
                target (e.g. "repos/my-repo"). When None, clones into
                the backend's root directory.

        Returns:
            GitManager instance for the cloned repo, or None on failure
        """
        masked = cls._mask_url_static(url)
        use_backend = backend is not None and getattr(backend, "supports_shell", False)

        if use_backend:
            try:
                if remote_cwd:
                    remote_target = posixpath.join(backend.root, remote_cwd)
                else:
                    remote_target = backend.root

                cmd = f"git clone {shlex.quote(url)} {shlex.quote(remote_target)}"
                output = backend.shell_run(
                    cmd, timeout=_CLONE_SHELL_TIMEOUT_SECONDS, tab_name="git"
                )

                first_line = output.split("\n", 1)[0].strip()
                if not first_line.startswith("Exit code: 0"):
                    if cls._clone_in_flight(output):
                        if not cls._wait_for_remote_clone(
                            backend, remote_target, masked
                        ):
                            return None
                    else:
                        logger.warning(f"git clone failed for {masked}: {output}")
                        return None

                mgr = cls(target_path, backend=backend, remote_cwd=remote_cwd)
                mgr._run_git(["config", "user.email", "agent@workspace.local"])
                mgr._run_git(["config", "user.name", "Agent"])

                logger.info(f"Cloned {masked} to {remote_target}")
                return mgr

            except Exception as e:
                logger.warning(f"Failed to clone {masked}: {e}")
                return None

        # --- Local subprocess path (no backend) ---

        return cls._clone_local(url, target_path, masked)

    @staticmethod
    def _clone_in_flight(output: str) -> bool:
        """True when shell_run reports the clone alive, not failed: either the
        wait cap expired mid-transfer, or the tab was already busy (a retry
        landing while an earlier clone still runs) and the command was never
        executed."""
        first_line = output.split("\n", 1)[0].strip()
        if first_line.startswith("Exit code: -1") and _STILL_RUNNING_MARKER in output:
            return True
        return _COLLIDING_MARKER in output and "NOT executed" in output

    @classmethod
    def _wait_for_remote_clone(cls, backend, remote_target: str, masked: str) -> bool:
        """Wait for an in-flight clone on the 'git' tab to finish, then verify.

        Polls with a probe that the busy tab rejects until the clone exits;
        once it runs, the probe's answer — is there a git repo at the target —
        is ground truth for whether the clone (whoever started it) succeeded.
        """
        probe = f"git -C {shlex.quote(remote_target)} rev-parse --git-dir"
        deadline = time.monotonic() + _CLONE_WAIT_DEADLINE_SECONDS
        logger.info(
            f"Clone of {masked} still transferring; waiting up to "
            f"{_CLONE_WAIT_DEADLINE_SECONDS}s for it to finish"
        )
        while time.monotonic() < deadline:
            time.sleep(_CLONE_POLL_INTERVAL_SECONDS)
            output = backend.shell_run(probe, timeout=60, tab_name="git")
            if cls._clone_in_flight(output):
                continue
            first_line = output.split("\n", 1)[0].strip()
            if first_line.startswith("Exit code: 0"):
                logger.info(f"In-flight clone of {masked} completed")
                return True
            logger.warning(
                f"Awaited clone of {masked} finished without a usable repo: {output}"
            )
            return False
        logger.warning(
            f"Gave up on clone of {masked} after {_CLONE_WAIT_DEADLINE_SECONDS}s "
            "— the git tab never freed up"
        )
        return False

    @classmethod
    def _clone_local(
        cls, url: str, target_path: Path, masked: str
    ) -> Optional["GitManager"]:
        """Clone via a local git subprocess (no backend)."""
        if shutil.which("git") is None:
            logger.warning("Cannot clone: git not available")
            return None

        try:
            result = subprocess.run(
                ["git", "clone", url, str(target_path)],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode != 0:
                logger.warning(f"git clone failed for {masked}: {result.stderr}")
                return None

            mgr = cls(target_path)
            # Configure local user for the cloned repo
            mgr._run_git(["config", "user.email", "agent@workspace.local"])
            mgr._run_git(["config", "user.name", "Agent"])

            logger.info(f"Cloned {masked} to {target_path}")
            return mgr

        except subprocess.TimeoutExpired:
            logger.warning(f"git clone timed out for {masked}")
            return None
        except Exception as e:
            logger.warning(f"Failed to clone {masked}: {e}")
            return None

    @classmethod
    def from_worktree(
        cls, worktree_path: Path, backend=None, remote_cwd=None
    ) -> Optional["GitManager"]:
        """Create a GitManager for an existing git worktree.

        Git worktrees have a `.git` file (not directory) that points to the
        parent repo's `.git` directory. This method validates the worktree
        exists and configures local user identity.

        Args:
            worktree_path: Path to the worktree directory
            backend: Optional WorkspaceBackend for remote execution
            remote_cwd: Relative path within backend root for the worktree

        Returns:
            GitManager instance, or None if path is not a valid git worktree
        """
        worktree_path = Path(worktree_path)
        use_backend = backend is not None and getattr(backend, "supports_shell", False)

        if use_backend:
            git_path = posixpath.join(remote_cwd, ".git") if remote_cwd else ".git"
            if not backend.exists(git_path):
                logger.warning(f"Not a git worktree (no .git): {worktree_path}")
                return None
        else:
            git_marker = worktree_path / ".git"
            if not git_marker.exists():
                logger.warning(f"Not a git worktree (no .git): {worktree_path}")
                return None

        mgr = cls(worktree_path, backend=backend, remote_cwd=remote_cwd)
        if not mgr._git_available:
            return None

        # Configure local user for the worktree
        mgr._run_git(["config", "user.email", "agent@workspace.local"])
        mgr._run_git(["config", "user.name", "Agent"])

        logger.info(f"GitManager initialized from worktree at {worktree_path}")
        return mgr

    @staticmethod
    def _mask_url_static(url: str) -> str:
        """Mask credentials in a URL for safe logging."""
        import re

        # "/" and "@" are excluded from both halves of the userinfo: they can
        # only appear percent-encoded there, and letting them through meant a
        # run of "://" markers made every one of them rescan the rest of the
        # URL for an "@" that never came (quadratic on hostile input).
        return re.sub(r"://([^:@/]+):[^@/]+@", r"://\1:***@", url)

    def _mask_url(self, url: str) -> str:
        """Instance method wrapper for URL masking."""
        return self._mask_url_static(url)

    def _run_git(
        self, args: List[str], timeout: int = DEFAULT_TIMEOUT
    ) -> subprocess.CompletedProcess:
        """Execute git command in workspace directory with timeout.

        When a workspace backend with shell support is available, the command
        is delegated to the backend (e.g. executed via SSH on a remote VM).
        Otherwise falls back to local subprocess execution.

        Args:
            args: Git command arguments (without 'git' prefix)
            timeout: Command timeout in seconds (default: 60)

        Returns:
            CompletedProcess with stdout, stderr, and returncode
        """
        if self._use_backend:
            cmd_str = "git " + " ".join(shlex.quote(a) for a in args)
            try:
                output = self._backend.shell_run(
                    cmd_str,
                    timeout=timeout,
                    tab_name="git",
                    working_dir=self._remote_cwd,
                )
                return self._parse_shell_run_output(output, args)
            except Exception as e:
                logger.error(f"Backend git command failed: {e}")
                return subprocess.CompletedProcess(
                    ["git"] + args, returncode=1, stdout="", stderr=str(e)
                )

        # --- Local subprocess path (no backend) ---
        cmd = ["git"] + args
        try:
            result = subprocess.run(
                cmd,
                cwd=self._workspace_path,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result
        except subprocess.TimeoutExpired:
            logger.error(f"Git command timed out after {timeout}s: {' '.join(cmd)}")
            return subprocess.CompletedProcess(
                cmd,
                returncode=1,
                stdout="",
                stderr=f"Command timed out after {timeout}s",
            )
        except Exception as e:
            logger.error(f"Git command failed: {e}")
            return subprocess.CompletedProcess(
                cmd, returncode=1, stdout="", stderr=str(e)
            )

    def _run_git_nul_records(
        self,
        args: List[str],
        timeout: int = DEFAULT_TIMEOUT,
    ) -> Optional[list[str]]:
        """Run Git and decode its NUL-delimited path records losslessly.

        Git's ``-z`` formats are required for filenames containing newlines.
        The tmux-backed remote shell is line-oriented, though, and strips raw
        NUL bytes. On a backend, base64 therefore wraps the byte stream before
        it crosses that transport; GitManager decodes it locally and validates
        the terminal record delimiter. The local subprocess path retains its
        historical direct ``-z`` behavior.

        Returns ``None`` on command/transport/decoding failure, otherwise the
        exact path records (with embedded newlines preserved).
        """

        if not self._use_backend:
            cmd = ["git"] + args
            try:
                result = subprocess.run(
                    cmd,
                    cwd=self._workspace_path,
                    capture_output=True,
                    text=False,
                    timeout=timeout,
                )
            except (subprocess.TimeoutExpired, OSError) as exc:
                logger.error("Local Git path command failed: %s", exc)
                return None
            if result.returncode != 0:
                return None
            raw_payload = result.stdout
        else:
            cmd_str = "git " + " ".join(shlex.quote(arg) for arg in args)
            encoded_cmd = (
                '( _srw_git_tmp=$(mktemp "${TMPDIR:-/tmp}/srw-git-nul.XXXXXX") '
                "|| exit 1; "
                "trap 'rm -f -- \"$_srw_git_tmp\"' EXIT; "
                "trap 'exit 129' HUP; trap 'exit 130' INT; trap 'exit 143' TERM; "
                f'{cmd_str} >"$_srw_git_tmp"; '
                "_srw_git_rc=$?; "
                '[ "$_srw_git_rc" -eq 0 ] || exit "$_srw_git_rc"; '
                '_srw_git_len=$(wc -c <"$_srw_git_tmp") || exit 1; '
                '_srw_git_sha=$(sha256sum "$_srw_git_tmp") || exit 1; '
                "_srw_git_sha=${_srw_git_sha%% *}; "
                'base64 "$_srw_git_tmp" || exit 1; '
                f"printf '{_GIT_NUL_INTEGRITY_FRAME} %s %s\\n' "
                '"$_srw_git_len" "$_srw_git_sha"; )'
            )
            try:
                output = self._backend.shell_run(
                    encoded_cmd,
                    timeout=timeout,
                    tab_name="git",
                    working_dir=self._remote_cwd,
                )
                encoded_result = self._parse_shell_run_output(output, args)
            except Exception as exc:
                logger.error("Backend Git path command failed: %s", exc)
                return None
            if encoded_result.returncode != 0:
                return None

            lines = encoded_result.stdout.splitlines()
            while lines and not lines[-1].strip():
                lines.pop()
            if not lines:
                logger.error("Backend Git path output lacked its integrity frame")
                return None
            frame_parts = lines.pop().split()
            if (
                len(frame_parts) != 3
                or frame_parts[0] != _GIT_NUL_INTEGRITY_FRAME
                or not frame_parts[1].isdigit()
                or len(frame_parts[2]) != 64
                or any(char not in "0123456789abcdef" for char in frame_parts[2])
            ):
                logger.error("Backend Git path output had an invalid integrity frame")
                return None
            expected_length = int(frame_parts[1])
            expected_sha256 = frame_parts[2]
            encoded = "".join(line.strip() for line in lines if line.strip())
            try:
                raw_payload = base64.b64decode(encoded, validate=True)
            except (binascii.Error, ValueError):
                logger.error("Backend Git path output was not valid base64")
                return None
            if (
                len(raw_payload) != expected_length
                or hashlib.sha256(raw_payload).hexdigest() != expected_sha256
            ):
                logger.error("Backend Git path output failed its integrity frame")
                return None

        if not raw_payload:
            return []
        if not raw_payload.endswith(b"\x00"):
            logger.error("Git -z path output ended without its record delimiter")
            return None
        return [
            record.decode("utf-8", errors="surrogateescape")
            for record in raw_payload[:-1].split(b"\x00")
        ]

    def _parse_shell_run_output(
        self, output: str, args: Optional[List[str]] = None
    ) -> subprocess.CompletedProcess:
        """Parse shell_run formatted output into a CompletedProcess.

        The backend's shell_run returns formatted strings like:
            "Exit code: 0\\n--- stdout ---\\nactual output..."
            "Exit code: 1\\n--- stdout ---\\nerror message..."
            "Exit code: 0\\n(no output)"

        Error cases (timeout, interactive prompt) lack the "Exit code:" prefix
        and are treated as failures.

        Args:
            output: Raw shell_run output string
            args: Git command args for the CompletedProcess.args field

        Returns:
            CompletedProcess with parsed returncode, stdout, and stderr
        """
        cmd = ["git"] + (args or [])
        lines = output.split("\n", 1)
        first_line = lines[0].strip()

        # A still-running result (soft/hard no-change timeout) is not a normal
        # completion. Surface it as a non-zero result with a clear stderr so git
        # callers don't mistake the explanatory text for command stdout.
        if "--- still running ---" in output:
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=-1,
                stdout="",
                stderr="git command did not complete (still running / timed out).\n"
                + output,
            )

        if first_line.startswith("Exit code:"):
            try:
                exit_code = int(first_line.split(":", 1)[1].strip())
            except (ValueError, IndexError):
                exit_code = 1

            rest = lines[1] if len(lines) > 1 else ""

            # Locate the payload marker rather than assuming it is the first
            # line. The backend prepends header lines before it — `CWD:` since
            # f41970ae — and the old `rest.startswith(...)` check fell through
            # to `stdout = rest`, handing callers the banner as command output.
            # That is how push() came to invoke
            # `git push -u origin "CWD: /…\n--- stdout ---\nmain"`, failing with
            # `invalid refspec` on every phase boundary. See
            # knowledge-base/knowledge/issues/deliverable_lost_to_nested_repo_commit_and_stranded_mode_a_job.md
            stdout_marker = "--- stdout ---\n"
            marker_at = rest.find(stdout_marker)
            if marker_at != -1:
                stdout = rest[marker_at + len(stdout_marker) :]
            else:
                # No payload marker: drop known header lines, then recheck for
                # the empty sentinel (which the header also displaced).
                body = "\n".join(
                    ln for ln in rest.split("\n") if not ln.startswith("CWD:")
                ).strip()
                stdout = "" if body == "(no output)" else body

            # The backend merges stdout and stderr into one stream, so on
            # failure the diagnosis is in `stdout`. Mirror it into `stderr` too:
            # callers log `result.stderr` on failure, and hardcoding "" is why
            # the real breakage surfaced for a day as `git push failed: ` with
            # nothing after the colon.
            return subprocess.CompletedProcess(
                args=cmd,
                returncode=exit_code,
                stdout=stdout,
                stderr="" if exit_code == 0 else stdout,
            )

        # No "Exit code:" prefix — timeout, stall, or other error
        return subprocess.CompletedProcess(
            args=cmd,
            returncode=1,
            stdout="",
            stderr=output,
        )

    def _truncate_output(
        self,
        output: str,
        max_lines: int = DEFAULT_MAX_LINES,
        max_words: int = DEFAULT_MAX_WORDS,
    ) -> str:
        """Truncate output to prevent context bloat.

        Args:
            output: Output string to truncate
            max_lines: Maximum number of lines (default: 500)
            max_words: Maximum number of words (default: 10000)

        Returns:
            Truncated output with count indicator if truncated
        """
        lines = output.splitlines()
        words = output.split()

        original_lines = len(lines)
        original_words = len(words)

        truncated = False
        result = output

        # Check lines first
        if len(lines) > max_lines:
            result = "\n".join(lines[:max_lines])
            truncated = True

        # Then check words (on potentially already-truncated result)
        result_words = result.split()
        if len(result_words) > max_words:
            result = " ".join(result_words[:max_words])
            truncated = True

        if truncated:
            result += f"\n\n... truncated ({original_lines} lines, {original_words} words total)"

        return result

    # --- Worktree management (for subagent delegation) ---

    def worktree_add(self, path: str, branch: str, create_branch: bool = True) -> bool:
        """Create a git worktree at the given path on the specified branch.

        Args:
            path: Relative path for the worktree (e.g., '.worktrees/subagent_0')
            branch: Branch name (e.g., 'subagent/0')
            create_branch: If True, create a new branch; if False, use existing

        Returns:
            True if worktree was created successfully
        """
        if not self.is_active:
            return False

        args = ["worktree", "add"]
        if create_branch:
            args.extend(["-b", branch, path])
        else:
            args.extend([path, branch])

        result = self._run_git(args)
        if result.returncode == 0:
            logger.info(f"Created worktree at {path} on branch {branch}")
            return True
        else:
            logger.error(f"Failed to create worktree at {path}: {result.stderr}")
            return False

    def worktree_remove(self, path: str, force: bool = False) -> bool:
        """Remove a git worktree.

        Args:
            path: Path to the worktree to remove
            force: If True, force removal even with uncommitted changes

        Returns:
            True if worktree was removed successfully
        """
        if not self.is_active:
            return False

        args = ["worktree", "remove"]
        if force:
            args.append("--force")
        args.append(path)

        result = self._run_git(args)
        if result.returncode == 0:
            logger.info(f"Removed worktree at {path}")
            return True
        else:
            logger.error(f"Failed to remove worktree at {path}: {result.stderr}")
            return False

    def worktree_list(self) -> list[dict]:
        """List all worktrees.

        Returns:
            List of dicts with 'path', 'head', and 'branch' keys
        """
        if not self.is_active:
            return []

        result = self._run_git(["worktree", "list", "--porcelain"])
        if result.returncode != 0:
            logger.warning(f"Failed to list worktrees: {result.stderr}")
            return []

        worktrees = []
        current: dict = {}
        for line in result.stdout.strip().splitlines():
            if line.startswith("worktree "):
                if current:
                    worktrees.append(current)
                current = {"path": line[len("worktree ") :].strip()}
            elif line.startswith("HEAD "):
                current["head"] = line[len("HEAD ") :].strip()
            elif line.startswith("branch "):
                current["branch"] = line[len("branch ") :].strip()
            elif line == "bare":
                current["bare"] = True
            elif line == "detached":
                current["detached"] = True
        if current:
            worktrees.append(current)

        return worktrees

    def merge_squash(self, branch: str) -> tuple[bool, str]:
        """Squash-merge the given branch into current HEAD.

        Performs `git merge --squash <branch>` then commits with a descriptive
        message. Does NOT delete the source branch — caller handles cleanup.

        Args:
            branch: Branch name to squash-merge

        Returns:
            Tuple of (success, message). Message contains the commit info on
            success, or the error/conflict details on failure.
        """
        if not self.is_active:
            return False, "Git not active"

        # Perform squash merge
        result = self._run_git(["merge", "--squash", branch])
        if result.returncode != 0:
            stderr = result.stderr.strip()
            if "CONFLICT" in result.stdout or "CONFLICT" in stderr:
                return False, f"Merge conflicts detected:\n{result.stdout}\n{stderr}"
            return False, f"Squash merge failed: {stderr}"

        # Check if merge reported no changes
        if "Already up to date" in result.stdout:
            return True, f"No changes from {branch} (already up to date)"

        # Commit the squash merge
        commit_msg = f"Squash merge {branch}"
        commit_result = self._run_git(["commit", "-m", commit_msg, "--no-edit"])
        if commit_result.returncode != 0:
            # Check if there's nothing to commit (no changes from branch)
            combined = commit_result.stdout + commit_result.stderr
            if "nothing to commit" in combined or "nothing added" in combined:
                return True, f"No changes from {branch} (already merged or empty)"
            return False, f"Commit after squash merge failed: {commit_result.stderr}"

        logger.info(f"Squash-merged branch {branch}")
        return True, f"Squash-merged {branch}: {commit_result.stdout.strip()}"

    def delete_branch(self, branch: str, force: bool = False) -> bool:
        """Delete a local branch.

        Args:
            branch: Branch name to delete
            force: If True, use -D (force delete); otherwise -d (safe delete)

        Returns:
            True if branch was deleted
        """
        if not self.is_active:
            return False

        flag = "-D" if force else "-d"
        result = self._run_git(["branch", flag, branch])
        if result.returncode == 0:
            logger.info(f"Deleted branch {branch}")
            return True
        else:
            logger.warning(f"Failed to delete branch {branch}: {result.stderr}")
            return False

    def cleanup_orphaned_worktrees(self) -> list[str]:
        """Remove worktrees in .worktrees/ that have no corresponding active branch.

        Scans git worktree list for entries under .worktrees/ and removes any
        that are no longer needed (branch deleted, or worktree in broken state).

        Returns:
            List of cleaned-up worktree paths
        """
        if not self.is_active:
            return []

        worktrees = self.worktree_list()
        cleaned = []

        for wt in worktrees:
            path = wt.get("path", "")
            if "/.worktrees/" not in path and "\\.worktrees\\" not in path:
                continue  # Not a delegation worktree

            # Check if worktree is in broken state (branch deleted, etc.)
            # git worktree list --porcelain shows "prunable" for stale entries
            # For simplicity, we use git worktree prune to clean up stale refs
            pass

        # Use git worktree prune to remove stale worktree refs
        prune_result = self._run_git(["worktree", "prune"])
        if prune_result.returncode == 0 and prune_result.stdout.strip():
            logger.info(f"Pruned stale worktree refs: {prune_result.stdout.strip()}")

        # Check for .worktrees/ directory entries that git no longer tracks
        worktrees_dir = self._workspace_path / ".worktrees"
        if worktrees_dir.exists() and worktrees_dir.is_dir():
            tracked_paths = {wt.get("path", "") for wt in self.worktree_list()}
            for child in worktrees_dir.iterdir():
                if child.is_dir() and str(child) not in tracked_paths:
                    # Orphaned directory — git doesn't know about it
                    try:
                        shutil.rmtree(child)
                        cleaned.append(str(child))
                        logger.info(f"Removed orphaned worktree directory: {child}")
                    except Exception as e:
                        logger.warning(
                            f"Failed to remove orphaned worktree {child}: {e}"
                        )

            # Remove .worktrees/ if empty
            if worktrees_dir.exists() and not any(worktrees_dir.iterdir()):
                try:
                    worktrees_dir.rmdir()
                    logger.info("Removed empty .worktrees/ directory")
                except Exception:
                    pass

        return cleaned

    def _check_git_available(self) -> bool:
        """Check if git is installed and available.

        Returns:
            True if git is available, False otherwise
        """
        return shutil.which("git") is not None
