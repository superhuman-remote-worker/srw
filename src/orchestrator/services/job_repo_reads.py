"""Read-only Gitea proxy for a job's workspace repository.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane C). Three
properties are load-bearing and moved unchanged:

* **The caller never sees Gitea credentials.** Every operation goes through
  the application's forge client, which owns the token; the cockpit asks for
  a job id and gets file/commit/tag data back.
* **Refusal order.** Authorization (``require_job_access``) is completed by
  the HTTP adapter *before* any of these operations runs, and each one then
  checks forge availability (503) before resolving the repo. A repo that
  cannot be resolved raises out of ``resolve_job_repo`` — the operations do
  not catch it.
* **The job's branch is the default ref.** ``ref or job_branch`` on the
  content reads, and ``sha if sha != "main" else (job_branch or sha)`` on the
  commit listing: a subjob asking for the literal default lands on its own
  branch, not on the root job's ``main``.

A file read answers ``encoding: "utf-8"`` with the text as ``content``, or,
for a file that is not valid UTF-8 (a PDF, an image, a SQLite database),
``encoding: "base64"`` with the bytes base64-encoded and ``size`` in bytes.
The text decode used to fail inside the forge client, so the route answered
404 for a file the directory listing showed.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import HTTPException


class RepoForge(Protocol):
    """The Gitea client surface these reads need, and nothing more."""

    @property
    def is_initialized(self) -> bool: ...

    async def list_contents(
        self, repo_name: str, path: str, *, ref: str | None = None
    ) -> list[dict[str, Any]] | None: ...

    async def get_file_bytes(
        self, repo_name: str, path: str, *, ref: str | None = None
    ) -> bytes | None: ...

    async def get_commits_between(
        self, repo_name: str, base: str, head: str
    ) -> dict[str, Any] | None: ...

    async def get_commits(
        self, repo_name: str, *, sha: str, page: int, limit: int
    ) -> list[dict[str, Any]] | None: ...

    async def get_diff(self, repo_name: str, base: str, head: str) -> str | None: ...

    async def get_tags(self, repo_name: str) -> list[dict[str, Any]] | None: ...


@dataclass(frozen=True)
class JobRepoReadDependencies:
    """Collaborators for one repo read, resolved per invocation.

    ``forge`` is rebound during ``lifespan`` and ``resolve_job_repo`` reads
    the store, so the application rebuilds this dataclass per request rather
    than capturing it at import.
    """

    forge: RepoForge
    resolve_job_repo: Callable[[str], Awaitable[tuple[str, str | None]]]


async def list_repo_contents(
    *,
    job_id: str,
    path: str,
    ref: str | None,
    dependencies: JobRepoReadDependencies,
) -> list[dict[str, Any]]:
    """List directory contents of a job's Gitea repository.

    Proxies the Gitea contents API so the cockpit doesn't need Gitea credentials.

    Returns:
        List of entries, each with: name, path, type ("file"|"dir"), size
    """
    if not dependencies.forge.is_initialized:
        raise HTTPException(
            status_code=503,
            detail="Gitea not available",
        )

    repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
    contents = await dependencies.forge.list_contents(
        repo_name, path, ref=ref or job_branch
    )

    if contents is None:
        raise HTTPException(
            status_code=404,
            detail=f"Path '{path or '/'}' not found in repo for job '{job_id}'",
        )

    return contents


async def get_repo_file(
    *,
    job_id: str,
    path: str,
    ref: str | None,
    dependencies: JobRepoReadDependencies,
) -> dict[str, Any]:
    """Get file content from a job's Gitea repository.

    Returns:
        Dict with path, content, encoding and size. ``encoding`` is
        ``"utf-8"`` (``content`` is the text, ``size`` its length) or
        ``"base64"`` (``content`` is a binary file's bytes base64-encoded,
        ``size`` the byte count).
    """
    if not dependencies.forge.is_initialized:
        raise HTTPException(
            status_code=503,
            detail="Gitea not available",
        )

    repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
    raw = await dependencies.forge.get_file_bytes(
        repo_name, path, ref=ref or job_branch
    )

    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"File '{path}' not found in repo for job '{job_id}'",
        )

    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {
            "path": path,
            "content": base64.b64encode(raw).decode("ascii"),
            "encoding": "base64",
            "size": len(raw),
        }

    return {
        "path": path,
        "content": content,
        "encoding": "utf-8",
        "size": len(content),
    }


async def list_repo_commits(
    *,
    job_id: str,
    sha: str,
    since_ref: str | None,
    page: int,
    limit: int,
    dependencies: JobRepoReadDependencies,
) -> dict[str, Any]:
    """List git commits for a job's repository.

    If since_ref is provided, returns only commits between since_ref and sha
    using git compare. Otherwise lists commits from sha.

    Returns:
        Dict with commits list and total count
    """
    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available")

    repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
    effective_sha = sha if sha != "main" else (job_branch or sha)

    if since_ref:
        # Commits-endpoint pagination with a client-side cut, not the compare
        # API: Gitea 1.22 compare 404s on SHA bases and always pays a
        # per-commit `git diff` server-side (ignores `stat=false`).
        compare = await dependencies.forge.get_commits_between(
            repo_name, since_ref, effective_sha
        )
        if compare is None:
            raise HTTPException(
                status_code=404,
                detail=f"Could not compare {since_ref}...{effective_sha} in repo for job '{job_id}'",
            )
        return compare
    else:
        commits = await dependencies.forge.get_commits(
            repo_name, sha=effective_sha, page=page, limit=limit
        )
        if commits is None:
            raise HTTPException(
                status_code=404,
                detail=f"No commits found in repo for job '{job_id}'",
            )
        return {"total_commits": len(commits), "commits": commits}


async def get_repo_diff(
    *,
    job_id: str,
    base: str,
    head: str,
    dependencies: JobRepoReadDependencies,
) -> dict[str, str]:
    """Get unified diff between two refs in a job's repository.

    Returns:
        Dict with base, head, and diff text
    """
    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available")

    repo_name, _job_branch = await dependencies.resolve_job_repo(job_id)
    diff_text = await dependencies.forge.get_diff(repo_name, base, head)

    if diff_text is None:
        raise HTTPException(
            status_code=404,
            detail=f"Could not get diff {base}...{head} in repo for job '{job_id}'",
        )

    return {"base": base, "head": head, "diff": diff_text}


async def list_repo_tags(
    *,
    job_id: str,
    all_jobs: bool,
    dependencies: JobRepoReadDependencies,
) -> list[dict[str, Any]]:
    """List tags in a job's repository.

    By default, only returns tags for the specified job (namespaced by
    job short ID prefix). Set all_jobs=True to return all tags in the repo.

    Returns:
        List of tags with name, sha, and message
    """
    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available")

    repo_name, _job_branch = await dependencies.resolve_job_repo(job_id)
    tags = await dependencies.forge.get_tags(repo_name)

    if tags is None:
        raise HTTPException(
            status_code=404,
            detail=f"No tags found in repo for job '{job_id}'",
        )

    # Filter to this job's tags unless all_jobs requested
    if not all_jobs:
        short_id = job_id[:8]
        tags = [t for t in tags if t["name"].startswith(f"{short_id}-")]

    return tags
