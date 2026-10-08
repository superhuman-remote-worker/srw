"""Workspace management for agent file-based storage.

This module provides a filesystem-based workspace abstraction for agents.
Each job gets an isolated workspace (container or VM) where agents can
store plans, documents, notes, and intermediate work products.

The workspace IS the base path — no per-job subdirectories. Isolation is
provided by the container/VM boundary, not directory structure.

Git versioning:
- Optional git repository per workspace for automatic change tracking
- Commits on todo completion for audit trail
- Phase tags for milestone tracking

Backend abstraction:
- File I/O is delegated to a WorkspaceBackend implementation
- RemoteBackend (SSH/SFTP) is the only production backend
- See knowledge-base/knowledge/features/vm_backend.md for the full design
"""

import logging
import os
import posixpath
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional, List
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from agent.managers.git_manager import GitManager
    from agent.core.backends.overlay import VirtualOverlayBackend
    from shared.runtime.core.workspace_backend import WorkspaceBackend

logger = logging.getLogger(__name__)

# Bounded retry for the job-repo clone. GitManager.clone returns None on any
# failure without distinguishing transient (a momentary reachability blip during
# an image rollout — the common case) from permanent (bad auth/URL), so we retry
# blindly a few times: a permanent failure only costs a couple of cheap extra
# attempts before the caller's F29 hard-fail. Removes the single-shot clone that
# turned a rollout-window blip into a dead project job.
# knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md
_CLONE_ATTEMPTS = 3
_CLONE_BACKOFF_SECONDS = (2.0, 5.0)  # waits before attempts 2 and 3


def _clone_repo_with_retry(
    url: str,
    target_path: "Path",
    *,
    backend=None,
    remote_cwd=None,
    attempts: int = _CLONE_ATTEMPTS,
) -> Optional["GitManager"]:
    """Clone with a bounded retry + backoff; returns the GitManager or None.

    ``_CLONE_BACKOFF_SECONDS`` and ``time.sleep`` are read at call time (not bound
    as defaults) so tests can stub the backoff to zero.
    """
    from agent.managers.git_manager import GitManager

    mgr = None
    for attempt in range(1, attempts + 1):
        mgr = GitManager.clone(url, target_path, backend=backend, remote_cwd=remote_cwd)
        if mgr is not None:
            if attempt > 1:
                logger.info("Clone succeeded on attempt %d/%d", attempt, attempts)
            return mgr
        if attempt < attempts:
            delay = _CLONE_BACKOFF_SECONDS[
                min(attempt - 1, len(_CLONE_BACKOFF_SECONDS) - 1)
            ]
            logger.warning(
                "Clone attempt %d/%d failed; retrying in %.0fs",
                attempt,
                attempts,
                delay,
            )
            time.sleep(delay)
    return mgr


def _normalize_repo_url(url: str) -> str:
    """Normalize a repo URL for equivalence checks.

    Drops credentials (tokens rotate between dispatches), a trailing slash,
    and an optional ``.git`` suffix. Non-URL forms (scp-like ``git@host:path``)
    fall back to the raw string.
    """
    raw = (url or "").strip()
    try:
        parts = urlsplit(raw)
        if not parts.scheme or not parts.hostname:
            return raw
        port = f":{parts.port}" if parts.port else ""
        path = parts.path.rstrip("/")
        if path.endswith(".git"):
            path = path[: -len(".git")]
        return f"{parts.scheme}://{parts.hostname}{port}{path}"
    except ValueError:
        return raw


def _managed_repository_path(url: str) -> Optional[str]:
    """Return the exact owner/repository path of a managed SSH transport.

    Only the new server-owned alias is accepted as the expected side of this
    comparison. This lets an existing workspace migrate from the previous
    release's credentialed HTTP origin to scoped SSH without treating the
    transport change as a foreign repository. Arbitrary SSH/HTTP URLs retain
    the strict full-origin comparison.
    """

    raw = (url or "").strip()
    try:
        parts = urlsplit(raw)
    except ValueError:
        return None
    hostname = parts.hostname or ""
    authority_suffix = hostname.removeprefix("srw-repo-")
    if (
        parts.scheme != "ssh"
        or not hostname.startswith("srw-repo-")
        or len(authority_suffix) != 32
        or any(character not in "0123456789abcdef" for character in authority_suffix)
        or parts.username is not None
        or parts.password is not None
    ):
        return None
    path = parts.path.rstrip("/")
    if path.endswith(".git"):
        path = path[: -len(".git")]
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) != 2:
        return None
    return "/".join(segments)


def _repository_path(url: str) -> Optional[str]:
    """Extract an exact URL repository path without treating it as authority."""

    try:
        parts = urlsplit((url or "").strip())
    except ValueError:
        return None
    if not parts.scheme or not parts.hostname:
        return None
    path = parts.path.rstrip("/")
    if path.endswith(".git"):
        path = path[: -len(".git")]
    segments = [segment for segment in path.split("/") if segment]
    if len(segments) != 2:
        return None
    return "/".join(segments)


@dataclass
class WorkspaceManagerConfig:
    """Configuration for WorkspaceManager.

    Note: This is distinct from loader.py:WorkspaceConfig which is used
    for AgentConfig JSON parsing. This class configures the runtime
    WorkspaceManager behavior.
    """

    # Base path for all workspaces
    base_path: Optional[str] = None

    # Standard subdirectories to create for each job
    structure: List[str] = field(
        default_factory=lambda: [
            "archive",
            "documents",
            "chunks",
            "candidates",
            "requirements",
            "output",
        ]
    )

    # Git versioning settings
    git_versioning: bool = True  # Enable git versioning for workspace history

    # Git remote URL for workspace delivery (set by orchestrator via Gitea)
    git_remote_url: Optional[str] = None

    # Project repository info
    branch_name: Optional[str] = None
    repositories: Optional[List[dict]] = None

    @classmethod
    def from_dict(cls, data: dict) -> "WorkspaceManagerConfig":
        """Create config from dictionary."""
        return cls(
            base_path=data.get("base_path"),
            structure=data.get(
                "structure", cls.__dataclass_fields__["structure"].default_factory()
            ),
            git_versioning=data.get("git_versioning", True),
            git_remote_url=data.get("git_remote_url"),
            branch_name=data.get("branch_name"),
            repositories=data.get("repositories"),
        )


_FALLBACK_BASE_WARNED = False


def get_workspace_base_path() -> Path:
    """Get the base path for the agent's local control-plane state.

    This is the agent process's own state directory (LangGraph checkpoints,
    per-job log files, phase snapshot tarballs) — NOT the job workspace
    where the LLM does its work. Job workspaces are always remote
    (SSH/SFTP into a workspace container/VM); see `WorkspaceManager`.

    Priority:
    1. WORKSPACE_PATH environment variable (set by container/pod manifests)
    2. /workspace if it exists (container/pod with PVC mounted there)
    3. ~/.cache/srw-agent/workspace for bare-metal dev (with a one-time warning)

    Returns:
        Path to workspace base directory
    """
    env_path = os.getenv("WORKSPACE_PATH")
    if env_path:
        return Path(env_path)

    container_path = Path("/workspace")
    if container_path.exists() and container_path.is_dir():
        return container_path

    global _FALLBACK_BASE_WARNED
    fallback = Path.home() / ".cache" / "srw-agent" / "workspace"
    if not _FALLBACK_BASE_WARNED:
        logger.warning(
            "WORKSPACE_PATH not set and /workspace not mounted; "
            "falling back to %s for agent state. "
            "Set WORKSPACE_PATH explicitly to silence this warning.",
            fallback,
        )
        _FALLBACK_BASE_WARNED = True
    return fallback


def get_checkpoints_path() -> Path:
    """Get path for LangGraph checkpoint storage.

    Checkpoints are stored in a shared directory outside individual job workspaces:
        workspace/checkpoints/job_<id>.db

    Returns:
        Path to checkpoints directory (created if it doesn't exist)
    """
    base = get_workspace_base_path()
    checkpoints_dir = base / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    return checkpoints_dir


def get_logs_path() -> Path:
    """Get path for job log file storage.

    Logs are stored in a shared directory outside individual job workspaces:
        workspace/logs/job_<id>.log

    Returns:
        Path to logs directory (created if it doesn't exist)
    """
    base = get_workspace_base_path()
    logs_dir = base / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


class WorkspaceManager:
    """Manages file-based workspaces for agent jobs.

    Each job gets an isolated workspace directory with a standard structure.
    The workspace provides persistent storage for plans, documents, notes,
    and intermediate work products.

    File I/O is delegated to a WorkspaceBackend instance, which the caller
    must provide. Production constructs a RemoteBackend that SSHes into a
    workspace container or VM. Tests use a filesystem-backed backend from
    tests/_fs_backend.py.

    Example:
        ```python
        # Create workspace for a job
        ws = WorkspaceManager(job_id="abc123", backend=backend)
        await ws.initialize()

        # Access paths
        plans_dir = ws.get_path("plans")
        output_file = ws.get_path("output/results.json")

        # Read/write files
        content = ws.read_file("plan.md")
        ws.write_file("research.md", "# Research Notes\\n...")

        # List contents
        files = ws.list_files("chunks")

        # Cleanup when done
        ws.cleanup()
        ```
    """

    def __init__(
        self,
        job_id: str,
        backend: "WorkspaceBackend",
        config: Optional[WorkspaceManagerConfig] = None,
        base_path: Optional[Path] = None,
    ):
        """Initialize workspace manager.

        Args:
            job_id: Unique job identifier (usually UUID)
            backend: Workspace backend. Production must pass a RemoteBackend;
                     the agent process never operates on its own filesystem.
            config: Optional workspace configuration
            base_path: Override base path (for testing)
        """
        if backend is None:
            raise TypeError(
                "WorkspaceManager requires a backend. Production must pass a "
                "RemoteBackend; tests should import FilesystemTestBackend from "
                "tests/_fs_backend.py."
            )

        self.job_id = job_id
        self.config = config or WorkspaceManagerConfig()

        # Determine base path
        if base_path:
            self._base_path = Path(base_path)
        elif self.config.base_path:
            self._base_path = Path(self.config.base_path)
        else:
            self._base_path = get_workspace_base_path()

        # Workspace path — the base path IS the workspace.
        # Isolation is provided by the container/VM, not subdirectories.
        self._workspace_path = self._base_path
        self._initialized = False

        # Virtual directories: wrap the real backend so registered prefixes
        # (tools/, contacts/, instructions.md) are served from live state.
        # See knowledge-base/knowledge/features/virtual_directories.md.
        # Unconditional, and there is no opt-out. `backend is None` already
        # raised above and nothing reassigns `_virtual_overlay` afterwards
        # (`swap_backend` rebinds it in place), so every manager has an overlay
        # for its whole life.
        #
        # This was gated on VIRTUAL_DIRS_ENABLED until 2026-08-07. The flag's
        # "off" position materialized instructions.md / task_brief.md into the
        # workspace root, which on a subjob sharing its parent's workspace put
        # the critic's brief where the TARGET reads it — see
        # knowledge-history/done/critic_brief_lands_in_shared_workspace_and_misleads_target.md.
        # It guarded against an agent booting with no task; src/graph.py now
        # guarantees that directly by refusing to start when both briefs resolve
        # empty, which covers every cause rather than this one.
        from agent.core.backends.overlay import VirtualOverlayBackend

        self._virtual_overlay: "VirtualOverlayBackend" = VirtualOverlayBackend(backend)
        self._backend = self._virtual_overlay

        # Git manager (created during initialize if git_versioning enabled)
        self._git_manager: Optional["GitManager"] = None

        # Source/reference repo git managers, keyed by repo name
        self._source_repos: dict[str, "GitManager"] = {}

        # Forge/auth metadata per cloned repo, keyed like _source_repos
        self._source_repo_meta: dict[str, dict] = {}

        # Why a repository connector was left uncloned, keyed by clone name
        # (the README says so instead of "cloned")
        self.source_repo_skipped: dict[str, str] = {}

    @property
    def backend(self) -> "WorkspaceBackend":
        """Get the workspace backend."""
        return self._backend

    @property
    def virtual_overlay(self):
        """The virtual overlay. Always present — see the constructor."""
        return self._virtual_overlay

    def register_virtual_provider(self, provider) -> None:
        """Register a virtual directory provider.

        Always registers: there is no flag that turns the overlay off, and a
        manager cannot exist without one. The guard this used to carry made a
        dropped provider silent, which is the wrong failure for a path that
        serves the agent its own instructions.
        """
        self._virtual_overlay.register(provider)

    def swap_backend(self, new_backend: "WorkspaceBackend") -> None:
        """Replace the real backend, keeping the virtual overlay in front of it.

        Workspace-tier upgrades (virtual -> sandbox in ``src/agent/agent.py``,
        container -> VM in ``src/agent/api/persistent_session.py``) hand the manager a
        different backend mid-run. Assigning ``self._backend`` directly drops
        the overlay: ``_virtual_overlay`` keeps wrapping the OLD, now
        disconnected backend, ``backend`` becomes the raw new one, and every
        virtual path 404s from then on — including the deferred-tool full docs.
        Re-registering providers cannot repair that, because they land on an
        overlay nothing reads through any more.

        Rebinding keeps the same overlay object (and therefore every registered
        provider) in place over the new backend.

        Args:
            new_backend: The connected replacement backend. Must be a real
                backend, never another overlay.
        """
        if new_backend is None:
            raise TypeError("swap_backend requires a backend")
        # Defensive: an overlay-in-an-overlay would double-route virtual paths.
        # Via the isinstance-typed helper, never getattr(_, "inner", _): a
        # MagicMock auto-creates `.inner`, so duck-typing here would swap in a
        # child mock instead of the backend the caller passed.
        from agent.core.backends.overlay import unwrap_backend

        new_backend = unwrap_backend(new_backend)
        self._virtual_overlay.rebind(new_backend)
        self._backend = self._virtual_overlay

    @property
    def path(self) -> Path:
        """Get the root path of this workspace."""
        return self._workspace_path

    @property
    def is_initialized(self) -> bool:
        """Check if workspace has been initialized."""
        return self._initialized or self._workspace_path.exists()

    @property
    def git_manager(self) -> Optional["GitManager"]:
        """Get the GitManager for this workspace.

        Returns None if git versioning is not enabled or initialization failed.
        """
        return self._git_manager

    @property
    def source_repos(self) -> dict[str, "GitManager"]:
        """Git managers for source/reference repos, keyed by repo name."""
        return self._source_repos

    @property
    def source_repo_meta(self) -> dict[str, dict]:
        """Forge/auth metadata per cloned repo, keyed like ``source_repos``.

        ``source_repos`` holds GitManager instances, which know how to run git
        but nothing about the forge's API. The repo tools need both.
        """
        return self._source_repo_meta

    def get_head_commit(self) -> Optional[str]:
        """Best-effort HEAD commit SHA at the workspace root.

        Used by the critic verdict tools to detect whether a target job's
        deliverable changed between review rounds (progress detection).
        Deliberately independent of ``git_manager``/``git_versioning``: a repo
        can be present at the workspace root (e.g. a project job's cloned jobs
        repo) even when workspace-level versioning was never enabled, so this
        always probes the root directly rather than relying on
        ``self._git_manager`` being set.

        This is a heuristic, not a correctness dependency — it must never
        raise. Any failure (no repo, git unavailable, backend error) returns
        None.
        """
        from agent.managers.git_manager import GitManager

        try:
            return GitManager(
                self._workspace_path, backend=self._backend
            ).get_current_commit()
        except Exception:
            return None

    def get_content_tree(self) -> Optional[str]:
        """Best-effort content hash of the committed workspace at HEAD.

        This — not ``get_head_commit`` — is what the verification gate's
        no-progress guard compares between rounds. A commit SHA moves on every
        round (both freeze branches commit with ``allow_empty=True``) and
        reverts on a re-clone after a failed push, so it is simultaneously
        unable to fire and able to fire backwards. See
        :meth:`GitManager.get_content_tree`.

        Probes the workspace root directly for the same reason
        ``get_head_commit`` does: a repo can be present there even when
        workspace-level versioning was never enabled. Never raises — any
        failure (no repo, git unavailable, backend error) returns None.
        """
        from agent.managers.git_manager import GitManager

        try:
            return GitManager(
                self._workspace_path, backend=self._backend
            ).get_content_tree()
        except Exception:
            return None

    @property
    def _backend_has_shell(self) -> bool:
        """Check if the workspace backend supports shell execution."""
        return getattr(self._backend, "supports_shell", False)

    def initialize(self) -> None:
        """Initialize the workspace directory structure.

        Creates the workspace root and all configured subdirectories.
        Safe to call multiple times - will not overwrite existing files.

        If git_versioning is enabled, also:
        - Creates a GitManager instance
        - Initializes a git repository with .gitignore
        - Creates initial phase_state.yaml
        - Makes an initial commit

        When a git_remote_url is configured, the workspace is cloned from
        the remote BEFORE creating subdirectories so that the local history
        extends the remote's (required for pushes and subjob branches).
        """
        # Workspace ownership and resetting belong to the provisioner. A new
        # harness can receive initialization files or a retained working tree;
        # starting an assignment must never empty that directory.
        # When we have a remote, attach/clone before adding framework directories.
        if self.config.git_versioning and self.config.git_remote_url:
            if not self._backend_has_shell:
                self._workspace_path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize_git()
            # Create any subdirectories the clone didn't provide
            for subdir in self.config.structure:
                self._backend.mkdir(subdir)
            self._initialized = True
            return

        # No remote — standard path: create dirs, then git init
        self._workspace_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized workspace at {self._workspace_path}")

        for subdir in self.config.structure:
            self._backend.mkdir(subdir)

        if self.config.git_versioning:
            self._initialize_git()

        self._initialized = True

    def _initialize_git(self) -> None:
        """Initialize git versioning for the workspace.

        If a git_remote_url is configured, clones the remote repo so that
        local history extends the remote's — this ensures pushes succeed and
        subjob branches (forked from the remote) inherit the workspace files.

        Falls back to ``git init`` + ``add_remote`` only when no remote URL
        is available.
        """
        from agent.managers.git_manager import GitManager

        if self.config.git_remote_url:
            # A retained root may already be a delivery repository. Keep its
            # history and uncommitted files, and reject a different origin
            # before any clone or remote rewrite.
            if self._backend.exists(".git"):
                self._git_manager = self._attach_existing_workspace_repo(
                    {"name": "workspace", "repo_url": self.config.git_remote_url}
                )
                if self.config.branch_name:
                    if not self._git_manager.checkout_branch(
                        self.config.branch_name, create=True
                    ):
                        raise RuntimeError("Could not select the workspace job branch.")
                return
            # Clone from remote so histories stay connected.
            git_mgr = GitManager.clone(
                self.config.git_remote_url,
                self._workspace_path,
                backend=self._backend,
            )
            if git_mgr:
                self._git_manager = git_mgr
                # Checkout job branch if specified
                if self.config.branch_name:
                    if git_mgr.checkout_branch(self.config.branch_name, create=True):
                        logger.info(f"Checked out branch: {self.config.branch_name}")
                    else:
                        logger.warning(
                            f"Failed to checkout branch {self.config.branch_name}"
                        )
                logger.info("Git versioning enabled (cloned from remote)")
                return
            # Clone failed — fall through to git init
            logger.warning("Failed to clone remote repo, falling back to git init")

        # No remote URL or clone failed: plain git init
        self._git_manager = GitManager(self._workspace_path, backend=self._backend)
        success = self._git_manager.init_repository()

        if success:
            logger.info("Git versioning enabled for workspace")
            if self.config.git_remote_url:
                self._git_manager.add_remote("origin", self.config.git_remote_url)
        else:
            logger.warning("Failed to initialize git repository")
            self._git_manager = None

    def initialize_project_workspace(self) -> None:
        """Initialize an isolated job workspace plus project attachments.

        Project membership no longer selects a shared workspace repository:
        1. Clone the job's own ``git_remote_url`` → workspace root
        2. Checkout job branch (create if needed)
        3. Create subdirectories
        4. Clone source/reference repos → repos/ subdirectory
        5. Update .gitignore to exclude cloned repos

        ``repositories`` now means auxiliary project attachments. Legacy jobs
        whose stored remote still names a shared jobs repo continue to work
        because the root is resolved from the job's own URL, not from a role.
        """
        repo_url = self.config.git_remote_url
        if not repo_url:
            logger.warning("Project job has no job repo URL; using local git init")
            self.initialize()
            self._clone_auxiliary_repos()
            return

        repo_path = str(repo_url).rstrip("/").rsplit("/", 1)[-1]
        workspace_repo = {
            "name": repo_path.removesuffix(".git") or "job-workspace",
            "repo_url": repo_url,
        }

        # 1. Reuse or clone the isolated job repo as workspace root.
        #
        # A FIRST dispatch can land on an already-populated root: on the
        # container backend the scholar subjob provisions the parent's shared
        # workspace pod and fully initializes it (job-repo clone + research)
        # before the parent ever runs, and critic/product-qa subjobs inherit a
        # populated parent workspace the same way. The agent-side reattach
        # gates are resume-only, so this path must cope on its own: cloning
        # into a non-empty root can never succeed, and retrying it just
        # produces the misleading reachability error below.
        git_mgr = self._attach_existing_workspace_repo(workspace_repo)

        # Empty root: clone (bounded retry+backoff — a momentary reachability
        # blip during an image rollout must not hard-fail the job on the first
        # miss; knowledge-history/done/coincident_infra_error_overrides_reported_job_outcome.md)
        if git_mgr is None:
            git_mgr = _clone_repo_with_retry(
                repo_url, self._workspace_path, backend=self._backend
            )
        if not git_mgr:
            # Do not silently git-init here. A job whose delivery repo will not
            # clone would rebuild in a disconnected local git and lose its work
            # at teardown. The name is safe to log; the URL may carry credentials.
            repo_name = workspace_repo["name"]
            raise RuntimeError(
                f"Failed to clone job workspace repo '{repo_name}' — refusing to "
                "fall back to a disconnected git init (work would be lost on "
                "teardown). Check repository reachability from this backend."
            )

        self._git_manager = git_mgr
        logger.info(f"Cloned job repo as workspace root: {self._workspace_path}")

        # 2. Checkout job branch
        branch = self.config.branch_name
        if branch:
            success = git_mgr.checkout_branch(branch, create=True)
            if success:
                logger.info(f"Checked out branch: {branch}")
            else:
                logger.warning(
                    f"Failed to checkout branch {branch}, continuing on default"
                )

        # 3. Create subdirectories
        for subdir in self.config.structure:
            self._backend.mkdir(subdir)

        # 4. Clone source/reference repos
        self._clone_auxiliary_repos()

        self._initialized = True
        logger.info("Project workspace initialized successfully")

    def _attach_existing_workspace_repo(
        self, workspace_repo: dict
    ) -> Optional["GitManager"]:
        """Attach to a pre-existing job-repo clone at the workspace root.

        Returns None when the root is empty (the normal clone path applies).
        Raises RuntimeError when the root is populated but cannot be reused:
        a clone could never succeed there, so fail immediately with an
        accurate message instead of burning the clone retries and reporting
        a reachability problem that doesn't exist.
        """
        from agent.managers.git_manager import GitManager

        repo_name = workspace_repo.get("name", "<unknown>")
        expected_url = workspace_repo["repo_url"]

        if not self._backend.exists(".git"):
            try:
                entries = self._backend.list_dir("")
            except Exception as e:
                logger.warning(
                    f"Workspace root listing failed ({e}); assuming empty root"
                )
                entries = []
            if entries:
                raise RuntimeError(
                    f"Workspace root is not empty ({len(entries)} entries) and has "
                    f"no git repo — refusing to clone job repo '{repo_name}' over "
                    "existing content. This usually means a pre-seeded or inherited "
                    "workspace was dispatched without resume handling."
                )
            return None

        git_mgr = GitManager(self._workspace_path, backend=self._backend)
        origin = git_mgr.remote_url("origin")
        same_origin = bool(
            origin and _normalize_repo_url(origin) == _normalize_repo_url(expected_url)
        )
        managed_path = _managed_repository_path(expected_url)
        legacy_managed_origin = bool(
            origin
            and managed_path is not None
            and _repository_path(origin) == managed_path
        )
        if origin and not same_origin and not legacy_managed_origin:
            # A PRESENT-but-different origin is a genuine identity conflict:
            # pushing would land this job's work in a foreign repo. Fail loud.
            raise RuntimeError(
                f"Workspace root holds a git repo whose origin "
                f"({GitManager._mask_url_static(origin)}) does not match "
                f"expected job repo '{repo_name}' — refusing to clone over "
                "it or push into it."
            )
        if legacy_managed_origin and not same_origin:
            logger.info(
                "Replacing a legacy managed HTTP origin with scoped SSH "
                "authority for job repo '%s'",
                repo_name,
            )
        elif not origin:
            # Unset/unreadable origin is NOT a conflict: `git remote get-url`
            # can fail transiently on a fresh session, and killing the job
            # here would lose the pre-seeded work (the exact failure F29
            # guards against). Log the raw git result for diagnosis, then
            # reuse the repo — add_remote below restores push connectivity.
            probe = git_mgr._run_git(["remote", "get-url", "origin"])
            logger.warning(
                "Existing job repo at workspace root has no readable origin "
                "(rc=%s, stdout=%r, stderr=%r) — attaching and setting origin "
                "to the expected job repo '%s'",
                probe.returncode,
                GitManager._mask_url_static(probe.stdout.strip())[-200:],
                GitManager._mask_url_static(probe.stderr.strip())[-200:],
                repo_name,
            )
        # Refresh origin so credentials rotated since the original clone
        # land in the repo config before any push.
        git_mgr.add_remote("origin", expected_url)
        logger.info(
            "Reusing existing job-repo clone at workspace root "
            "(pre-initialized workspace, e.g. scholar-provisioned pod)"
        )
        return git_mgr

    def _clone_auxiliary_repos(self) -> None:
        """Clone source/reference repositories into repos/ subdirectory.

        Two roles are deliberately never cloned: legacy ``jobs`` (the job's
        stored remote already owns the workspace root) and ``knowledge`` (the KB vault is server-side —
        notes are read through the KB tools and written via the
        orchestrator's materialisation endpoint, so a checkout would be a
        stale second copy nobody reads;
        knowledge-base/knowledge/features/knowledge_base_repo_separation.md §3).
        """
        if not self.config.repositories:
            return

        from agent.managers.git_manager import GitManager

        self._backend.mkdir("repos")
        repos_dir = self._workspace_path / "repos"

        for repo in self.config.repositories:
            if repo["role"] == "jobs":
                continue  # Legacy project workspace; never an auxiliary clone
            if repo["role"] == "knowledge":
                continue  # KB vault is server-side; never checked out

            repo_name = repo["name"]
            target = repos_dir / repo_name
            remote_cwd = f"repos/{repo_name}"
            repo_url = repo.get("repo_url")

            if self._backend.exists(remote_cwd):
                if repo.get("is_managed"):
                    # A workspace created by the previous release can already
                    # contain this auxiliary clone with the shared administrator
                    # URL in its .git/config.  Merely skipping the clone would
                    # preserve that bearer forever across resume.  Accept only
                    # the same exact owner/repository path, then replace origin
                    # with the newly proven scoped SSH alias before any tool can
                    # inspect or use it.
                    managed_path = _managed_repository_path(str(repo_url or ""))
                    git_mgr = GitManager(
                        target,
                        backend=self._backend,
                        remote_cwd=remote_cwd,
                    )
                    if managed_path is None or not git_mgr.is_active:
                        raise RuntimeError(
                            f"Managed auxiliary repository '{repo_name}' has no "
                            "usable scoped Git authority"
                        )
                    origin = git_mgr.remote_url("origin")
                    if origin and _repository_path(origin) != managed_path:
                        raise RuntimeError(
                            f"Managed auxiliary repository '{repo_name}' has a "
                            "foreign origin; refusing to replace it"
                        )
                    if not git_mgr.add_remote("origin", str(repo_url)):
                        raise RuntimeError(
                            f"Managed auxiliary repository '{repo_name}' origin "
                            "could not be refreshed"
                        )
                    self._source_repos[repo_name] = git_mgr
                logger.debug(f"Repo {repo_name} already cloned, skipping")
                continue

            if not repo_url:
                logger.warning(f"Repo {repo_name} has no URL, skipping")
                continue

            try:
                git_mgr = GitManager.clone(
                    repo_url,
                    target,
                    backend=self._backend,
                    remote_cwd=remote_cwd,
                )
                if git_mgr:
                    branch = repo.get("branch", "main")
                    if branch and branch != "main":
                        git_mgr.checkout_branch(branch)
                    self._source_repos[repo_name] = git_mgr
                    logger.info(f"Cloned {repo['role']} repo: {repo_name}")
                else:
                    if repo.get("is_managed"):
                        raise RuntimeError(
                            f"Managed auxiliary repository '{repo_name}' could "
                            "not be cloned with scoped authority"
                        )
                    logger.warning(f"Failed to clone repo: {repo_name}")
            except Exception as e:
                if repo.get("is_managed"):
                    raise RuntimeError(
                        f"Managed auxiliary repository '{repo_name}' setup failed"
                    ) from e
                logger.warning(f"Error cloning repo {repo_name}: {e}")

        # Update .gitignore to exclude repos/ directory
        if self._backend.exists(".gitignore"):
            content = self._backend.read_file(".gitignore")
            if "repos/" not in content:
                self._backend.append_file(
                    ".gitignore", "\n# Cloned project repositories\nrepos/\n"
                )
        else:
            self._backend.write_file(
                ".gitignore", "# Cloned project repositories\nrepos/\n"
            )

        if self._git_manager:
            self._git_manager.commit("Add repos/ to .gitignore", allow_empty=False)

    def get_path(self, relative_path: str = "") -> Path:
        """Get absolute path within workspace.

        Delegates path validation to the backend, returns a Path object
        for backward compatibility with callers that need filesystem paths.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            Absolute path within workspace

        Raises:
            ValueError: If path attempts to escape workspace
        """
        return Path(self._backend.resolve_path(relative_path))

    def workspace_relative_path(self, path: str | os.PathLike[str]) -> str:
        """Return one canonical workspace-relative identity for ``path``.

        File tools historically returned the backend's canonical path while
        most storage APIs accepted a workspace-relative path.  Remote
        backends therefore surfaced absolute paths and virtual backends
        surfaced object keys.  Treating either value as already relative can
        escape the workspace root or double-prefix an object key.

        This method accepts either representation, strips only the *exact*
        canonical backend root, and rejects absolute/out-of-root or ``..``
        paths.  Callers can consequently use the returned value consistently
        for backend I/O and durable path-keyed state.
        """

        raw = os.fspath(path).strip()
        if not raw:
            return ""

        root_raw = str(self._backend.resolve_path(""))
        root = posixpath.normpath(root_raw) if root_raw else ""
        candidate = posixpath.normpath(raw)

        root_is_absolute = bool(root and posixpath.isabs(root))
        if root and candidate == root:
            relative = ""
        elif root and candidate.startswith(root.rstrip("/") + "/"):
            stripped = candidate[len(root.rstrip("/")) + 1 :]
            if root_is_absolute:
                relative = stripped
            else:
                # A virtual backend's canonical object key is relative-looking.
                # It can collide with a legitimate workspace-relative path
                # that happens to begin with the same prefix. Read/citation
                # callers always address an existing object, so disambiguate
                # from backend authority and refuse a two-object alias.
                stripped_exists = self._backend.exists(stripped)
                unstripped_exists = self._backend.exists(candidate)
                if stripped_exists == unstripped_exists:
                    reason = "ambiguous" if stripped_exists else "missing"
                    raise ValueError(
                        f"Path '{raw}' has {reason} workspace-relative identity"
                    )
                relative = stripped if stripped_exists else candidate
        else:
            if posixpath.isabs(candidate):
                raise ValueError(f"Path '{raw}' is outside the workspace root")
            relative = candidate

        if relative in ("", "."):
            return ""
        if posixpath.isabs(relative) or relative == ".." or relative.startswith("../"):
            raise ValueError(f"Path '{raw}' escapes workspace boundary")

        # Let the concrete backend enforce any stricter path vocabulary too.
        self._backend.resolve_path(relative)
        return relative

    @contextmanager
    def local_copy(self, relative_path: str) -> Generator[Path, None, None]:
        """Yield a local filesystem path to a workspace file.

        Materialize through the backend unconditionally.  Whether a resolved
        path happens to exist in the *agent* process is not evidence that the
        workspace is local: remote and virtual paths can collide with files in
        the agent image/CWD.  The small temporary copy is therefore the common
        correctness path for every backend and is cleaned up on exit.

        Use this whenever a service (AudioHelper, VisionHelper, PDFReader,
        etc.) needs to open a file with local I/O.

        Args:
            relative_path: Path relative to workspace root

        Yields:
            A Path on the local filesystem containing the file data
        """
        data = self._backend.read_file(relative_path, binary=True)
        if isinstance(data, str):
            data = data.encode()
        suffix = Path(relative_path).suffix
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        try:
            tmp.write(data)
            tmp.close()
            yield Path(tmp.name)
        finally:
            Path(tmp.name).unlink(missing_ok=True)

    def exists(self, relative_path: str) -> bool:
        """Check if a file or directory exists in workspace.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            True if path exists
        """
        return self._backend.exists(relative_path)

    def read_file(self, relative_path: str) -> str:
        """Read a file from the workspace.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            File contents as string

        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If path escapes workspace
        """
        return self._backend.read_file(relative_path)

    def write_file(self, relative_path: str, content: str) -> Path:
        """Write content to a file in the workspace.

        Creates parent directories if they don't exist.

        Args:
            relative_path: Path relative to workspace root
            content: Content to write

        Returns:
            Absolute path to written file

        Raises:
            ValueError: If path escapes workspace
        """
        self._backend.write_file(relative_path, content)
        return self.get_path(relative_path)

    def append_file(self, relative_path: str, content: str) -> Path:
        """Append content to a file in the workspace.

        Creates the file if it doesn't exist.

        Args:
            relative_path: Path relative to workspace root
            content: Content to append

        Returns:
            Absolute path to file
        """
        self._backend.append_file(relative_path, content)
        return self.get_path(relative_path)

    def create_directory(self, relative_path: str) -> Path:
        """Create a directory (and parents) in workspace.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            Absolute path to created directory

        Raises:
            ValueError: If path escapes workspace
        """
        self._backend.mkdir(relative_path)
        return self.get_path(relative_path)

    def delete_directory(self, relative_path: str) -> bool:
        """Delete a directory and all its contents.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            True if deleted, False if didn't exist

        Raises:
            ValueError: If path escapes workspace or is the workspace root
        """
        return self._backend.delete_directory(relative_path)

    def delete_file(self, relative_path: str) -> bool:
        """Delete a file or empty directory from workspace.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            True if deleted, False if didn't exist

        Raises:
            ValueError: If trying to delete non-empty directory
        """
        return self._backend.delete_file(relative_path)

    def move_file(self, source: str, dest: str) -> Path:
        """Move a file or directory within the workspace.

        Creates parent directories for destination if needed.
        Can also be used to rename files.

        Args:
            source: Source path relative to workspace root
            dest: Destination path relative to workspace root

        Returns:
            Absolute path to the moved file/directory

        Raises:
            FileNotFoundError: If source doesn't exist
            ValueError: If paths escape workspace boundary
        """
        self._backend.move(source, dest)
        return self.get_path(dest)

    def copy_file(self, source: str, dest: str) -> Path:
        """Copy a file within the workspace.

        Creates parent directories for destination if needed.

        Args:
            source: Source path relative to workspace root
            dest: Destination path relative to workspace root

        Returns:
            Absolute path to the copied file

        Raises:
            FileNotFoundError: If source doesn't exist
            ValueError: If paths escape workspace boundary or source is a directory
        """
        self._backend.copy(source, dest)
        return self.get_path(dest)

    def list_files(self, relative_path: str = "", pattern: str = "*") -> List[str]:
        """List files in a workspace directory.

        Args:
            relative_path: Path relative to workspace root
            pattern: Glob pattern to filter files (default: "*")

        Returns:
            List of relative paths to files/directories
        """
        return self._backend.list_dir(relative_path, pattern)

    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> List[dict]:
        """Search for text in workspace files.

        Args:
            query: Text to search for
            path: Directory to search in (default: entire workspace)
            case_sensitive: Whether search is case-sensitive
            exclude_dirs: Optional list of directory names to skip with grep

        Returns:
            List of dicts with 'path', 'line_number', and 'line' for each match
        """
        return self._backend.search_files(
            query,
            path,
            case_sensitive,
            exclude_dirs=exclude_dirs,
        )

    def get_size(self, relative_path: str = "") -> int:
        """Get size of a file or directory in bytes.

        Args:
            relative_path: Path relative to workspace root

        Returns:
            Size in bytes
        """
        return self._backend.stat(relative_path)

    def cleanup(self) -> bool:
        """Remove the entire workspace directory.

        Returns:
            True if workspace was removed, False if it didn't exist
        """
        if not self._workspace_path.exists():
            return False

        shutil.rmtree(self._workspace_path)
        logger.info(f"Cleaned up workspace: {self._workspace_path}")
        self._initialized = False

        return True

    def get_summary(self) -> dict:
        """Get a summary of workspace contents.

        Returns:
            Dictionary with file counts and sizes by directory
        """
        summary = {
            "job_id": self.job_id,
            "path": "/",  # Always return "/" - agent sees workspace as root
            "exists": self._workspace_path.exists(),
            "directories": {},
            "total_files": 0,
            "total_size_bytes": 0,
        }

        if not self._workspace_path.exists():
            return summary

        for subdir in self.config.structure:
            subdir_clean = subdir.rstrip("/")
            if self._backend.is_dir(subdir_clean):
                entries = self._backend.list_dir(subdir_clean)
                # Count only files (entries without trailing /)
                file_count = len([e for e in entries if not e.endswith("/")])
                dir_size = self._backend.stat(subdir_clean)

                summary["directories"][subdir] = {
                    "file_count": file_count,
                    "size_bytes": dir_size,
                }
                summary["total_files"] += file_count
                summary["total_size_bytes"] += dir_size

        return summary

    def __repr__(self) -> str:
        return (
            f"WorkspaceManager(job_id='{self.job_id}', path='{self._workspace_path}')"
        )
