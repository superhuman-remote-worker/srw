"""Project repository tools for persistent sessions."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from langchain_core.tools import tool

from agent.tools.context import ToolContext
from shared.orch_surface.formatters import truncate_text as _truncate

from agent.tools.orchestrator.jobs import _get_client, _get_orchestrator_url

from shared.tool_catalog.definitions import (
    REPOSITORY_TOOLS_METADATA as REPOSITORY_TOOLS_METADATA,
)

logger = logging.getLogger(__name__)


def _current_project_id(context: ToolContext) -> Optional[str]:
    return context.project_id or (
        context.project_ids[0] if context.project_ids else None
    )


def _format_repository(repo: Dict[str, Any]) -> List[str]:
    repo_id = repo.get("id", "unknown")
    lines = [
        f"--- {repo_id} ---",
        f"  Name: {_truncate(repo.get('name'), limit=160)}",
        f"  Role: {repo.get('role', 'unknown')}",
    ]
    if repo.get("project_id"):
        lines.append(f"  Project ID: {repo['project_id']}")
    if repo.get("description"):
        lines.append(f"  Description: {_truncate(repo.get('description'), limit=220)}")
    if repo.get("branch"):
        lines.append(f"  Branch: {repo['branch']}")
    if repo.get("clone_path"):
        lines.append(f"  Clone path: {repo['clone_path']}")
    if repo.get("read_only") is not None:
        lines.append(f"  Read-only: {repo['read_only']}")
    if repo.get("is_managed") is not None:
        lines.append(f"  Managed: {repo['is_managed']}")
    if repo.get("repo_url"):
        lines.append(f"  Display URL: {repo['repo_url']}")
    return lines


def _select_repository(
    repos: List[Dict[str, Any]],
    *,
    repo_id: Optional[str] = None,
    name: Optional[str] = None,
    role: Optional[str] = None,
) -> Dict[str, Any] | None:
    if repo_id:
        for repo in repos:
            if str(repo.get("id")) == str(repo_id):
                return repo
        return None

    if name:
        normalized = name.strip().lower()
        for repo in repos:
            if str(repo.get("name", "")).lower() == normalized:
                return repo
        return None

    if role is not None:
        for repo in repos:
            if repo.get("role") == role:
                return repo
        return None
    return repos[0] if repos else None


def _safe_checkout_path(repo: Dict[str, Any], target_path: Optional[str]) -> str:
    candidate = target_path or repo.get("clone_path")
    if not candidate:
        name = str(repo.get("name") or "project-repo")
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip(".-") or "project-repo"
        candidate = f"repos/{slug}"

    path = str(candidate).strip().strip("/")
    if not path or path == ".":
        raise ValueError(
            "Refusing to clone into workspace root; choose a subdirectory."
        )
    if path.startswith("/") or any(part == ".." for part in Path(path).parts):
        raise ValueError("target_path must be a relative path inside the workspace.")
    return path


def _ensure_checkout_path_ignored(backend, checkout_path: str) -> None:
    """Gitignore the freshly cloned nested repo so the per-turn `git add -A`
    doesn't stage it as a contentless gitlink (bug b1758f38). Mirrors the job
    path (src/agent/core/workspace.py:800-813) but derives the entry from the
    ACTUAL checkout_path (target_path is caller-configurable), not a
    hardcoded repos/. Writes the session workspace ROOT .gitignore.
    Idempotent. Never raises.
    """
    # Leading '/' anchors the entry to the root .gitignore's directory;
    # trailing '/' restricts the match to a directory.
    entry = f"/{checkout_path}/"
    header = "# Cloned project repositories (working-tree only; never versioned)"
    try:
        if backend.exists(".gitignore"):
            content = backend.read_file(".gitignore")
            if isinstance(content, bytes):
                content = content.decode("utf-8", "replace")
            if entry in {line.strip() for line in content.splitlines()}:
                return
            backend.append_file(".gitignore", f"\n{entry}\n")
        else:
            backend.write_file(".gitignore", f"{header}\n{entry}\n")
    except Exception as e:
        logger.warning("Failed to gitignore checkout path %s: %s", checkout_path, e)


async def _fetch_public_repositories(
    client: httpx.AsyncClient,
    base_url: str,
    project_id: str,
    *,
    role: Optional[str] = None,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if role:
        params["role"] = role
    resp = await client.get(
        f"{base_url}/api/projects/{project_id}/repositories",
        params=params,
    )
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("repositories", [])


async def _fetch_internal_repositories(
    client: httpx.AsyncClient,
    base_url: str,
    context: ToolContext,
    project_id: str,
) -> List[Dict[str, Any]]:
    if not context.thread_id:
        return []
    resp = await client.get(
        f"{base_url}/api/agents/threads/{context.thread_id}/workspace",
        # Only the repositories are read; the header keeps the poll from
        # being taken for an agent older than the in-pod plane (D7).
        headers={"X-SRW-Cloud-Mount-Delivery": "sidecar"},
    )
    resp.raise_for_status()
    data = resp.json()
    repos = data.get("repositories") or []
    return [repo for repo in repos if str(repo.get("project_id")) == str(project_id)]


def create_repository_tools(context: ToolContext) -> List[Any]:
    """Create repository tools with injected session context."""
    base_url = _get_orchestrator_url()

    @tool
    async def list_project_repositories(
        project_id: Optional[str] = None,
        role: Optional[str] = None,
    ) -> str:
        """List repositories attached to a project.

        Args:
            project_id: Optional project UUID. Defaults to this session's project.
            role: Optional role filter, such as jobs, source, or reference.
        """
        effective_project_id = project_id or _current_project_id(context)
        if not effective_project_id:
            return (
                "No project_id was provided and this session is not scoped to "
                "a current project."
            )

        async with _get_client(user_id=context.user_id) as client:
            try:
                repos = await _fetch_public_repositories(
                    client, base_url, effective_project_id, role=role
                )
                if not repos:
                    filter_msg = f" with role='{role}'" if role else ""
                    return (
                        f"No repositories found for project {effective_project_id}"
                        f"{filter_msg}."
                    )

                lines = [
                    f"Found {len(repos)} repository row(s) for project "
                    f"{effective_project_id}:\n"
                ]
                for repo in repos:
                    lines.extend(_format_repository(repo))
                    lines.append("")
                return "\n".join(lines)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Project '{effective_project_id}' not found or not visible."
                return f"Failed to list project repositories: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def get_default_project_repository(project_id: Optional[str] = None) -> str:
        """Show the current project's preferred writable source repository."""
        effective_project_id = project_id or _current_project_id(context)
        if not effective_project_id:
            return (
                "No project_id was provided and this session is not scoped to "
                "a current project."
            )

        async with _get_client(user_id=context.user_id) as client:
            try:
                repos = await _fetch_public_repositories(
                    client, base_url, effective_project_id
                )
                repo = _select_repository(repos, role="source")
                if not repo:
                    return (
                        "No writable source repository is linked to project "
                        f"{effective_project_id}. Project files live in the "
                        "project cloud folder, not in a default Git workspace."
                    )
                return "\n".join(_format_repository(repo))
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    return f"Project '{effective_project_id}' not found or not visible."
                return (
                    f"Failed to get default repository: HTTP {e.response.status_code}"
                )
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

    @tool
    async def checkout_project_repository(
        project_id: Optional[str] = None,
        repo_id: Optional[str] = None,
        name: Optional[str] = None,
        role: str = "source",
        target_path: Optional[str] = None,
    ) -> str:
        """Clone a project repository into this session workspace.

        Args:
            project_id: Optional project UUID. Defaults to this session's project.
            repo_id: Optional repository UUID to clone.
            name: Optional repository name to clone.
            role: Source/reference repository role to select when repo_id/name
                are omitted. Managed knowledge and legacy jobs repositories are
                never cloned into a new session workspace.
            target_path: Optional relative checkout path. Defaults to repos/<name>.
        """
        effective_project_id = project_id or _current_project_id(context)
        if not effective_project_id:
            return (
                "No project_id was provided and this session is not scoped to "
                "a current project."
            )

        workspace = context.workspace_manager
        backend = getattr(workspace, "backend", None) if workspace else None
        if not workspace or not getattr(backend, "supports_shell", False):
            return (
                "Repository checkout requires a shell-capable sandbox or VM "
                "workspace. Use request_workspace_upgrade first."
            )

        async with _get_client(user_id=context.user_id) as client:
            try:
                repos = await _fetch_internal_repositories(
                    client, base_url, context, effective_project_id
                )
            except httpx.HTTPStatusError as e:
                return f"Failed to resolve internal repositories: HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                return f"Failed to connect to orchestrator: {e}"

        repo = _select_repository(repos, repo_id=repo_id, name=name, role=role)
        if not repo:
            return f"No matching repository found for project {effective_project_id}."
        if repo.get("role") in {"jobs", "knowledge"}:
            return (
                f"Repository {repo.get('name') or repo.get('id')} has managed "
                f"role '{repo.get('role')}' and is not checkout-eligible. "
                "Only linked source/reference repositories belong in a new "
                "agent workspace."
            )
        repo_url = repo.get("repo_url")
        if not repo_url:
            return f"Repository {repo.get('name') or repo.get('id')} has no clone URL."

        try:
            checkout_path = _safe_checkout_path(repo, target_path)
        except ValueError as e:
            return str(e)

        if backend.exists(checkout_path):
            return (
                f"Repository {repo.get('name')} is already present at "
                f"{checkout_path}. Use shell/git tools there to inspect or update it."
            )

        try:
            from agent.managers.git_manager import GitManager

            git_mgr = GitManager.clone(
                repo_url,
                workspace.path / checkout_path,
                backend=backend,
                remote_cwd=checkout_path,
            )
            if not git_mgr:
                return (
                    f"Failed to clone repository {repo.get('name')}. "
                    "Check repository reachability from the workspace backend."
                )
            branch = repo.get("branch") or "main"
            if branch and branch != "main":
                git_mgr.checkout_branch(branch)
            _ensure_checkout_path_ignored(backend, checkout_path)
            return (
                "Repository checked out.\n"
                f"Project ID: {effective_project_id}\n"
                f"Repository ID: {repo.get('id')}\n"
                f"Name: {repo.get('name')}\n"
                f"Role: {repo.get('role')}\n"
                f"Branch: {branch}\n"
                f"Path: {checkout_path}\n"
                f"Read-only: {repo.get('read_only')}"
            )
        except Exception as e:
            return f"Failed to checkout repository {repo.get('name')}: {e}"

    return [
        list_project_repositories,
        get_default_project_repository,
        checkout_project_repository,
    ]
